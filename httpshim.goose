// The part of goose-http that pool.goose uses, over goose-io's contract (github.com/BHare1985/goose-io), so that
// the pool runs on any provider of it with its source as it is: `httpshim::` where it had `http::`.
//
// goose-http's calls act on the loop they run in, so this module keeps that loop's state in globals (a thread
// has its own), and queues what the pool asks for: the host runs it on its provider once per event.
//
//   import goose_io from "github.com/BHare1985/goose-io@<tag>";
//   import pg.pool;
//
//   io.serve() { io, ev =>
//       httpshim::event(io, ev);              // every event: sockets, timers
//       loop {                                // each wake is a call of its exchange's route
//           var w = httpshim::Wake { .. };
//           if !httpshim::next(w) { break; }
//           route(w.key, w.kind, w.data[..]); // pg::query(db, t, httpshim::Reply { key: w.key }, kind, data, ...)
//       }
//       httpshim::flush(io);                  // after every route call: what they asked for goes out
//   };
//
// An exchange is a request of the host's, with a key the host chooses (not 0) and ends with httpshim::end(key).
// The pool's links carry httpshim::TAG. Addresses are numeric, as the contract's are, or `unix:/path` on an event loop
// that supplies the unix group (a host of the pool that begins with / names the directory of the server's socket).
namespace httpshim;

import std;
import goose_io;

let TAG: u16 = 0x7067;

let SOCKETS = 16;
let SOCK_IN = 65536;
let SOCK_OUT = 16384;
let EXCHANGES = 1024;
let WAKES = 256;

// What a route is called for (Event.kind), a wait's events, and the results of mail_send.
let REQUEST: u8 = 0;
let WAKE: u8 = 1;
let TIMEOUT: u8 = 2;
let CLOSED: u8 = 3;
let READABLE: u32 = 1;
let WRITABLE: u32 = 4;
let SENT = 0;
let FULL = 1;
let GONE = 2;

let EAGAIN = 11;
let EINPROGRESS = 115;
let ECONNREFUSED = 111;
let ETIMEDOUT = 110;
let ENOENT = 2;
let EBADF = 9;
let ENOBUFS = 105;

struct Reply { key: u64 = 0 }
struct MailAddress { key: u64 = 0 }
struct Socket { id: i64 = -1, gen: u32 = 0 }
struct Error { code: i64 = 0, call: const u8[:] = "" }

// What a route is called with: the exchange, why, and a mail's bytes.
struct Wake { key: u64 = 0, kind: u8 = 0, data: u8[..8] }

struct Sock {
    used: bool,
    gen: u32,
    pending: bool,      // to dial
    dialled: bool,
    up: bool,
    eof: bool,          // Closed delivered
    hung: bool,         // the pool hung up: reset it, and free it at Closed
    resetting: bool,
    errno: i64,
    link: goose_io::Link,
    waiter: u64,
    addr: u8[..108],
    out: u8[..SOCK_OUT],
    in_len: i64,
    inbuf: u8[SOCK_IN],
}

struct Exch { key: u64 = 0, at: i64 = 0 }

var socks: Sock[SOCKETS];
var exch: Exch[EXCHANGES];
var wakes: Wake[WAKES];
var wake_head: i64;
var wake_count: i64;
var clock: i64;

// --- what the pool calls ------------------------------------------------------------------------------------

fn connect_start(where: const u8[:], port: i64) -> Socket, Error {
    for i in SOCKETS {
        if !socks[i].used {
            let s .= socks[i];
            s.used = true;
            s.gen++;
            s.pending = true;
            s.dialled = false;
            s.up = false;
            s.eof = false;
            s.hung = false;
            s.resetting = false;
            s.errno = 0;
            s.waiter = 0;
            s.out.clear();
            s.in_len = 0;
            s.addr.clear();
            if starts_with(where, "unix:") { s.addr.append(where); }
            else if find(where, ":") >= 0 { format(s.addr, "[", where, "]:", port); }
            else { format(s.addr, where, ":", port); }
            return Socket { id: i, gen: s.gen }, Error { .. };
        }
    }
    return Socket { .. }, Error { code: ENOBUFS, call: "connect" };
}

fn connected(s: Socket) -> Error {
    let i = _index(s);
    guard i >= 0 else { return Error { code: EBADF, call: "connect" }; }
    if socks[i].up { return Error { .. }; }
    if socks[i].eof { return Error { code: if socks[i].errno != 0 { socks[i].errno } else { ECONNREFUSED }, call: "connect" }; }
    return Error { code: EINPROGRESS, call: "connect" };
}

// Queues bytes for the next flush: all of them, or -1 for none.
fn send(s: Socket, bytes: const u8[:]) -> i64 {
    let i = _index(s);
    guard i >= 0 && socks[i].up && !socks[i].eof && !socks[i].hung && socks[i].out.len + bytes.len <= SOCK_OUT else { return -1; }
    socks[i].out.append(bytes);
    return bytes.len;
}

// What has arrived, up to into.len bytes; 0 at the server's close, -EAGAIN for none yet.
fn recv(s: Socket, into: u8[:]) -> i64 {
    let i = _index(s);
    guard i >= 0 else { return -EBADF; }
    let n = min(into.len, socks[i].in_len);
    if n > 0 {
        for j in n { into[j] = socks[i].inbuf[j]; }
        for j in socks[i].in_len - n { socks[i].inbuf[j] = socks[i].inbuf[n + j]; }
        socks[i].in_len -= n;
        return n;
    }
    return if socks[i].eof { 0 } else { -EAGAIN };
}

fn hangup(s: Socket) {
    let i = _index(s);
    guard i >= 0 else { return; }
    if !socks[i].dialled || socks[i].eof { socks[i].used = false; } else { socks[i].hung = true; }
}

// The exchange's route is called, with WAKE, when the socket is ready for events, or with TIMEOUT after ms.
fn wait_socket(out: Reply, s: Socket, events: u32, ms: i64) -> Error {
    let i = _index(s);
    guard i >= 0 else { return Error { code: EBADF, call: "wait_socket" }; }
    _arm(out.key, ms);
    let ready = ((events & READABLE) != 0 && (socks[i].in_len > 0 || socks[i].eof)) || ((events & WRITABLE) != 0 && (socks[i].up || socks[i].eof));
    if ready { _wake(out.key, WAKE, ""); } else { socks[i].waiter = out.key; }
    return Error { .. };
}

// The exchange's route is called with TIMEOUT after ms; its address is for mail.
fn later(out: Reply, ms: i64) -> MailAddress {
    _arm(out.key, ms);
    return MailAddress { key: out.key };
}

fn mail_live(a: MailAddress) -> bool { return _exch(a.key) >= 0; }

// Calls the exchange's route with WAKE and these bytes (up to 8).
fn mail_send(a: MailAddress, tag: i32, bytes: const u8[:]) -> i64 {
    guard _exch(a.key) >= 0 else { return GONE; }
    guard wake_count < WAKES else { return FULL; }
    _wake(a.key, WAKE, bytes);
    return SENT;
}

// --- what the host calls ------------------------------------------------------------------------------------

// Takes in an event: the clock and the timers that are due, and the sockets' news. True if it was the pool's.
fn event<Io>(io: Io&, ev: goose_io::Event) -> bool {
    clock = io.now_ms();
    for i in EXCHANGES {
        if exch[i].key != 0 && exch[i].at != 0 && clock >= exch[i].at {
            exch[i].at = 0;
            _wake(exch[i].key, TIMEOUT, "");
        }
    }
    guard goose_io::is_link_event(ev) && goose_io::tag_of(ev) == TAG else { return false; }
    let user = goose_io::user_of(ev);
    guard user >= 1 && user <= SOCKETS else { return true; }
    let i = (user - 1) as i64;
    guard socks[i].used && goose_io::same(socks[i].link, goose_io::link_of(ev)) else { return true; }
    match ev {
        Opened e => { socks[i].up = true; },
        Data e => {
            if socks[i].in_len + e.bytes.len > SOCK_IN {
                socks[i].hung = true;
            } else {
                for j in e.bytes.len { socks[i].inbuf[socks[i].in_len + j] = e.bytes[j]; }
                socks[i].in_len += e.bytes.len;
            }
        },
        Closed e => {
            socks[i].eof = true;
            socks[i].errno = _errno(e.why);
        },
        _ => { return true; },
    }
    if socks[i].waiter != 0 {
        _wake(socks[i].waiter, WAKE, "");
        socks[i].waiter = 0;
    }
    if socks[i].eof && socks[i].hung { socks[i].used = false; }
    return true;
}

// The next call a route is due, oldest first.
fn next(w: Wake&) -> bool {
    guard wake_count > 0 else { return false; }
    w = wakes[wake_head];
    wake_head = (wake_head + 1) % WAKES;
    wake_count--;
    return true;
}

// Runs what the routes asked for: dials, sends, and hang-ups.
fn flush<Io>(io: Io&) {
    for i in SOCKETS {
        let s .= socks[i];
        guard s.used else { continue; }
        if s.pending {
            s.pending = false;
            s.dialled = true;
            s.link = io.dial(s.addr[..], TAG, 0);
            io.set_user(s.link, (i + 1) as u32);
        }
        if s.hung && s.dialled && !s.eof && !s.resetting {
            s.resetting = true;
            io.reset(s.link);
        }
        while s.up && !s.eof && !s.hung && s.out.len > 0 {
            let n = min(s.out.len, io.max_send());
            guard io.send(s.link, s.out[..n]) == goose_io::SendStatus.Sent else { break; }
            for j in s.out.len - n { s.out[j] = s.out[n + j]; }
            s.out.resize(s.out.len - n, 0);
        }
    }
}

// The exchange is over: no more calls of its route.
fn end(key: u64) {
    let e = _exch(key);
    if e >= 0 { exch[e].key = 0; }
    for i in SOCKETS { if socks[i].waiter == key { socks[i].waiter = 0; } }
}

// --- the shim's own -----------------------------------------------------------------------------------------

fn _index(s: Socket) -> i64 {
    guard s.id >= 0 && s.id < SOCKETS && socks[s.id].used && socks[s.id].gen == s.gen else { return -1; }
    return s.id;
}

fn _exch(key: u64) -> i64 {
    for i in EXCHANGES { if exch[i].key == key && key != 0 { return i; } }
    return -1;
}

fn _arm(key: u64, ms: i64) {
    var e = _exch(key);
    if e < 0 {
        for i in EXCHANGES { if exch[i].key == 0 { e = i; break; } }
    }
    guard e >= 0 else { return; }
    exch[e].key = key;
    exch[e].at = if ms > 0 { clock + ms } else { 0 };
}

fn _wake(key: u64, kind: u8, data: const u8[:]) {
    guard wake_count < WAKES else { return; }
    let w .= wakes[(wake_head + wake_count) % WAKES];
    w.key = key;
    w.kind = kind;
    w.data.clear();
    w.data.append(data[..min(data.len, 8)]);
    wake_count++;
}

fn _errno(why: goose_io::CloseReason) -> i64 {
    match why {
        ConnectFailed f => match f.kind {
            Refused => { return ECONNREFUSED; },
            TimedOut => { return ETIMEDOUT; },
            _ => { return ECONNREFUSED; },
        },
        _ => { return 0; },
    }
}
