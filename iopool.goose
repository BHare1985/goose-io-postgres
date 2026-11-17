// A pool of connections on a goose-io loop (github.com/BHare1985/goose-io), for a program whose
// event loop is any goose-io provider rather than goose-http's. It is a goose-io library: a state
// struct the host owns, and one entry point, io_event, that the host calls with every event and that
// says whether the event was its own. It imports the contract and no provider. A program imports
// the module pg.iopool, and declares goose-io in its root file.
//
//   import goose_io from "github.com/BHare1985/goose-io@<tag>";
//   import pg.iopool;
//
//   var db = pg::io_pool(cfg, TAG, 4);          // before serve
//   let id = pg::io_submit(db, io, "SELECT id, name FROM items WHERE id = $1", 5000) { w =>
//       pg::param(w, 42);
//   };                                          // 0: no room, or the SQL or parameters don't fit
//
//   io.serve() { io, ev =>
//       db.io_event(io, ev) { id, r => ... pg::int(r, 0), pg::text(r, 1) ... };
//       loop {
//           let done = db.io_take();
//           if !done.found { break; }
//           // done.id, done.failed, done.rows, done.code, done.message
//       }
//   };
//
// One query runs on a connection at a time, as the unnamed statement; queries wait for an idle
// connection, and a connection is opened when one waits and none is coming, up to the pool's size.
// Rows are given to the block as they arrive, so a query that fails after some rows has given those
// first. The address is numeric, as the contract's are: a host name or a Unix socket path fails the
// query with "08001".
namespace pg;

import std;
import goose_io;
import hash from "github.com/kaitk/goose-hash@v0.0.4";

// The most connections a pool opens, and the most queries it holds at once: queued, running, or
// finished and not yet taken with io_take.
let IO_SLOTS = 16;
let IO_REQUESTS = 128;
// Each connection's buffer: the most one message may take, so the longest message it accepts.
let IO_INPUT = 65536;
// The longest SQL, and the most bytes of parameters (each with its 4-byte length), a query holds.
let IO_SQL = 1024;
let IO_PARAMS = 1024;

// A connection's phases.
let IO_EMPTY: u8 = 0;       // no link
let IO_DIALLING: u8 = 1;
let IO_STARTING: u8 = 2;    // the startup message sent; authenticating
let IO_IDLE: u8 = 3;
let IO_BUSY: u8 = 4;        // a query is out
let IO_CLOSING: u8 = 5;     // close or reset called; the link's Closed is to come

// A query's states.
let IO_FREE: u8 = 0;
let IO_QUEUED: u8 = 1;
let IO_SENT: u8 = 2;

let IO_ADMIN_SHUTDOWN = "57P01";

// A query's parameters, written by the block of io_submit: pg::param(w, v) with the same values
// the facade's query takes.
struct IoParams {
    count: i64 = 0,
    over: bool = false,
    bytes: u8[..IO_PARAMS],
}

// Appends v as 4 bytes, most significant first.
fn _io_length(w: IoParams&, v: i64) {
    for k in 4 { w.bytes.push(((v >> (24 - 8 * k)) & 0xff) as! u8); }
}

fn param(w: IoParams&, v: const u8[:]) {
    guard w.bytes.len + 4 + v.len <= IO_PARAMS else { w.over = true; return; }
    _io_length(w, v.len);
    w.bytes.append(v);
    w.count++;
}

fn param(w: IoParams&, v: i64) {
    var text: u8[..24] = [];
    format(text, v);
    param(w, text[..]);
}

fn param(w: IoParams&, v: bool) { param(w, if v { "t" } else { "f" }); }

// What the block of io_submit gets: a reference to the parameters, since a block's argument is a copy.
struct IoWriter { p: IoParams& }

fn param(w: IoWriter, v) { param(w.p, v); }

fn param(w: IoParams&, v: Null) {
    guard w.bytes.len + 4 <= IO_PARAMS else { w.over = true; return; }
    _io_length(w, -1);
    w.count++;
}

struct IoRequest {
    state: u8 = IO_FREE,
    id: u32 = 0,
    // When the query gives up, in the loop's clock: 0 for never.
    deadline: i64 = 0,
    params: i64 = 0,
    sql: u8[..IO_SQL],
    bytes: u8[..IO_PARAMS],
}

// What io_take returns for each query that has ended: its rows were given to the block of io_event
// as they arrived. `code` is a SQLSTATE, the server's or one of the facade's, when `failed`.
struct IoResult {
    found: bool = false,
    id: u32 = 0,
    failed: bool = false,
    rows: i64 = 0,
    code: u8[..5],
    message: u8[..120],
}

struct IoSlot {
    link: goose_io::Link = goose_io::Link { .. },
    phase: u8 = IO_EMPTY,
    req: i64 = -1,          // the request a busy connection runs
    rows: i64 = 0,
    failed: bool = false,
    code: u8[..5],
    message: u8[..120],
    // input[at..len] holds what has arrived and step hasn't read.
    at: i64 = 0,
    len: i64 = 0,
    c: Conn,
    input: u8[IO_INPUT],
}

// The pool's state. Build it with io_pool, keep it last in what holds it: the connections end it.
struct IoPool {
    cfg: Config,
    tag: u16 = 0,
    connect_ms: i64 = 5000,
    address: u8[..64],
    next_id: u32 = 1,
    live: i64 = 0,
    reqs: IoRequest[IO_REQUESTS],
    done: IoResult[IO_REQUESTS],
    done_head: i64 = 0,
    done_count: i64 = 0,
    slots: IoSlot[>..],
}

// A pool of up to size connections (1 to IO_SLOTS) to cfg's server, none open yet, whose links
// carry tag. A connection takes messages up to IO_INPUT - 1 bytes, or cfg.max_message if lower.
fn io_pool(cfg: Config, tag: u16, size: i64, connect_ms: i64 = 5000) -> IoPool {
    var p = IoPool { cfg: cfg, tag: tag, connect_ms: connect_ms, .. };
    p.cfg.max_message = min(cfg.max_message, IO_INPUT - 1);
    if find(cfg.host[..], ":") >= 0 {
        format(p.address, "[", cfg.host[..], "]:", cfg.port);
    } else {
        format(p.address, cfg.host[..], ":", cfg.port);
    }
    for i in min(max(size, 1), IO_SLOTS) { p.slots.push(IoSlot { c: Conn { .. }, .. }); }
    return p;
}

// Queues sql with no parameters; see io_submit with a block. ms limits the whole query, from this
// call to its end; 0 is no limit. Returns the query's id, or 0 when the pool holds IO_REQUESTS
// queries, is draining, or the SQL is longer than IO_SQL or holds a NUL byte.
fn io_submit<Io>(p: IoPool&, io: Io&, sql: const u8[:], ms: i64) -> u32 {
    return io_submit(p, io, sql, ms) { w => };
}

// As above, with the block writing the parameters: `{ w => pg::param(w, 1); pg::param(w, "x"); }`.
// Returns 0 too when they take more than IO_PARAMS bytes.
fn io_submit<Io, W>(p: IoPool&, io: Io&, sql: const u8[:], ms: i64) -> u32 {
    guard !io.draining() && p.live < IO_REQUESTS else { return 0; }
    guard sql.len <= IO_SQL && !has_nul(sql) else { return 0; }
    var w = IoParams { .. };
    W(IoWriter { p: w });
    guard !w.over else { return 0; }
    var k = -1;
    for i in IO_REQUESTS { if p.reqs[i].state == IO_FREE { k = i; break; } }
    guard k >= 0 else { return 0; }
    let r .= p.reqs[k];
    let id = p.next_id;
    p.next_id = if id == 0xffffffff { 1 } else { id + 1 };
    r.state = IO_QUEUED;
    r.id = id;
    r.deadline = if ms > 0 { io.now_ms() + ms } else { 0 };
    r.params = w.count;
    r.sql.clear();
    r.sql.append(sql);
    r.bytes.clear();
    r.bytes.append(w.bytes[..]);
    p.live++;
    p._pump(io);
    return id;
}

// The next query that has ended, oldest first, or found == false.
fn io_take(p: IoPool&) -> IoResult {
    guard p.done_count > 0 else { return IoResult { .. }; }
    let r = p.done[p.done_head];
    p.done_head = (p.done_head + 1) % IO_REQUESTS;
    p.done_count--;
    p.live--;
    return r;
}

// The pool's one entry point: true if the event was for this pool, false if it is another
// library's (or a Tick or PassEnd, which every library is offered). F is called with each row, as
// `{ id, r => ... }`, with the id io_submit returned.
fn io_event<Io, F>(p: IoPool&, io: Io&, ev: goose_io::Event) -> bool {
    match ev {
        Tick t => {
            p._expire(io);
            if io.draining() { p._drain(io); }
            return false;
        },
        PassEnd => { return false; },
        Opened e => {
            if e.tag != p.tag { return false; }
            let i = p._slot_of(e.user, e.link);
            if i >= 0 { p._opened(io, i); }
            return true;
        },
        Data e => {
            if e.tag != p.tag { return false; }
            let i = p._slot_of(e.user, e.link);
            if i >= 0 { p._data(io, i, e.bytes, F); }
            return true;
        },
        Writable e => { return e.tag == p.tag; },
        PeerEof e => { return e.tag == p.tag; },
        Closed e => {
            if e.tag != p.tag { return false; }
            let i = p._slot_of(e.user, e.link);
            if i >= 0 { p._closed(io, i, e.why); }
            return true;
        },
    }
}

fn _slot_of(p: IoPool&, user: u32, link: goose_io::Link) -> i64 {
    guard user >= 1 && user <= p.slots.len else { return -1; }
    let i = (user - 1) as i64;
    guard p.slots[i].phase != IO_EMPTY && goose_io::same(p.slots[i].link, link) else { return -1; }
    return i;
}

// Hands each queued query to an idle connection, then opens a connection for the queries that
// still have none coming.
fn _pump<Io>(p: IoPool&, io: Io&) {
    loop {
        let k = p._oldest_queued();
        guard k >= 0 else { return; }
        var idle = -1;
        for i in p.slots.len { if p.slots[i].phase == IO_IDLE { idle = i; break; } }
        guard idle >= 0 else { break; }
        p._send_query(io, idle, k);
    }
    var queued = 0;
    for i in IO_REQUESTS { if p.reqs[i].state == IO_QUEUED { queued++; } }
    var coming = 0;
    for i in p.slots.len { if p.slots[i].phase == IO_DIALLING || p.slots[i].phase == IO_STARTING { coming++; } }
    while queued > coming && !io.draining() {
        var free = -1;
        for i in p.slots.len { if p.slots[i].phase == IO_EMPTY { free = i; break; } }
        guard free >= 0 else { return; }
        let s .= p.slots[free];
        s.link = io.dial(p.address[..], p.tag, p.connect_ms);
        io.set_user(s.link, (free + 1) as u32);
        s.phase = IO_DIALLING;
        s.at = 0;
        s.len = 0;
        coming++;
    }
}

// The queued query with the lowest id sent first: ids rise with each submit, so the first free
// index after a wrap is not the oldest; the queue is small, so it is searched.
fn _oldest_queued(p: IoPool&) -> i64 {
    var best = -1;
    for i in IO_REQUESTS {
        if p.reqs[i].state == IO_QUEUED && (best < 0 || p._before(p.reqs[i].id, p.reqs[best].id)) { best = i; }
    }
    return best;
}

// Whether id a was issued before id b, across the wrap of the counter.
fn _before(p: IoPool&, a: u32, b: u32) -> bool { return (b - a) < 0x80000000; }

fn _opened<Io>(p: IoPool&, io: Io&, i: i64) {
    let s .= p.slots[i];
    var nonce: u8[18] = [0; 18];
    hash::random_bytes(nonce);
    var o: u8[>..] = [];
    start(s.c, o, p.cfg, nonce);
    s.at = 0;
    s.len = 0;
    guard s.c.state != BROKEN else { p._close_slot(io, i, SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION, s.c.reason); return; }
    s.phase = IO_STARTING;
    p._put(io, i, o);
}

// Sends o on slot i's link; a refusal closes the connection, which fails what it carries.
fn _put<Io>(p: IoPool&, io: Io&, i: i64, o: const u8[:]) {
    let s .= p.slots[i];
    guard o.len > 0 else { return; }
    if io.send(s.link, o) != goose_io::SendStatus.Sent {
        p._close_slot(io, i, CONNECTION_FAILURE, "sending to the server failed");
    }
}

// Writes request k's entries and sends them on idle slot i.
fn _send_query<Io>(p: IoPool&, io: Io&, i: i64, k: i64) {
    let s .= p.slots[i];
    let r .= p.reqs[k];
    var o: u8[>..] = [];
    parse(s.c, o, "", r.sql[..]);
    let b = bind(o, "");
    if fits(o, b, r.bytes.len) {
        o.append(r.bytes[..]);
        count(o, b, r.params);
    }
    execute(s.c, o, b);
    sync(s.c, o);
    guard s.c.state != BROKEN else {
        p._finish(k, true, 0, CONNECTION_FAILURE, s.c.reason);
        p._close_slot(io, i, CONNECTION_FAILURE, s.c.reason);
        return;
    }
    r.state = IO_SENT;
    s.phase = IO_BUSY;
    s.req = k;
    s.rows = 0;
    s.failed = false;
    s.code.clear();
    s.message.clear();
    p._put(io, i, o);
}

fn _data<Io, F>(p: IoPool&, io: Io&, i: i64, bytes: const u8[:]) {
    let s .= p.slots[i];
    if s.len + bytes.len > IO_INPUT { p._compact(i); }
    guard s.len + bytes.len <= IO_INPUT else {
        p._close_slot(io, i, CONNECTION_FAILURE, "the answer is longer than IO_INPUT");
        return;
    }
    for j in bytes.len { s.input[s.len + j] = bytes[j]; }
    s.len += bytes.len;
    loop {
        guard s.phase != IO_CLOSING && s.phase != IO_EMPTY else { return; }
        var o: u8[>..] = [];
        let e, body = step(s.c, s.input[s.at..s.len], o);
        s.at += s.c.used;
        if o.len > 0 { p._put(io, i, o); }
        if e == WANT_INPUT { break; }
        if e == FAILED {
            let code = field(body, 'C');
            p._close_slot(io, i, if code.len > 0 { code } else { CONNECTION_FAILURE },
                          if code.len > 0 { field(body, 'M') } else { s.c.reason });
            return;
        }
        if s.phase == IO_STARTING {
            if e == READY {
                s.phase = IO_IDLE;
                p._pump(io);
            }
        } else if s.phase == IO_BUSY {
            if e == ROW {
                F(p.reqs[s.req].id, Row { c: s.c, body: body });
            } else if e == DONE {
                s.rows = rows(body);
            } else if e == ERROR && !s.failed {
                s.failed = true;
                s.code.append(field(body, 'C')[..min(field(body, 'C').len, 5)]);
                s.message.append(field(body, 'M')[..min(field(body, 'M').len, 120)]);
            } else if e == READY {
                let k = s.req;
                s.req = -1;
                s.phase = IO_IDLE;
                p._finish(k, s.failed, s.rows, s.code[..], s.message[..]);
                p._pump(io);
            }
        }
    }
    p._compact(i);
}

// Moves what step hasn't read to the front of slot i's input.
fn _compact(p: IoPool&, i: i64) {
    let s .= p.slots[i];
    guard s.at > 0 else { return; }
    let keep = s.len - s.at;
    for j in keep { s.input[j] = s.input[s.at + j]; }
    s.at = 0;
    s.len = keep;
}

// Ends request k and queues its result for io_take.
fn _finish(p: IoPool&, k: i64, failed: bool, rows: i64, code: const u8[:], message: const u8[:]) {
    let r .= p.reqs[k];
    let d .= p.done[(p.done_head + p.done_count) % IO_REQUESTS];
    d.found = true;
    d.id = r.id;
    d.failed = failed;
    d.rows = rows;
    d.code.clear();
    d.code.append(code[..min(code.len, 5)]);
    d.message.clear();
    d.message.append(message[..min(message.len, 120)]);
    p.done_count++;
    r.state = IO_FREE;
}

// Fails what slot i carries, and closes it. The link's Closed frees the slot.
fn _close_slot<Io>(p: IoPool&, io: Io&, i: i64, code: const u8[:], why: const u8[:]) {
    let s .= p.slots[i];
    if s.phase == IO_BUSY && s.req >= 0 { p._finish(s.req, true, 0, code, why); }
    s.req = -1;
    s.phase = IO_CLOSING;
    io.reset(s.link);
}

fn _closed<Io>(p: IoPool&, io: Io&, i: i64, why: goose_io::CloseReason) {
    let s .= p.slots[i];
    if s.phase == IO_BUSY && s.req >= 0 {
        p._finish(s.req, true, 0, CONNECTION_FAILURE, "the server closed the connection during the query");
    } else if s.phase == IO_DIALLING && goose_io::is_connect_failed(why) {
        // The oldest query that waits is the one this dial was for; failing it keeps a server that
        // refuses from being dialled again and again.
        let k = p._oldest_queued();
        if k >= 0 {
            var message: u8[>..] = [];
            format(message, "can't connect to ", p.address[..]);
            p._finish(k, true, 0, SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION, message[..]);
        }
    }
    s.phase = IO_EMPTY;
    s.req = -1;
    s.at = 0;
    s.len = 0;
    p._pump(io);
}

// Fails the queries whose time has passed: a running one resets its connection.
fn _expire<Io>(p: IoPool&, io: Io&) {
    let now = io.now_ms();
    for i in IO_REQUESTS {
        let r .= p.reqs[i];
        if r.state == IO_FREE || r.deadline == 0 || now < r.deadline { continue; }
        if r.state == IO_QUEUED {
            p._finish(i, true, 0, CONNECTION_FAILURE, "the database didn't answer in time");
        } else {
            for j in p.slots.len {
                if p.slots[j].phase == IO_BUSY && p.slots[j].req == i {
                    p._close_slot(io, j, CONNECTION_FAILURE, "the database didn't answer in time");
                }
            }
        }
    }
}

// While the loop is stopping: ends the idle sessions and fails the queries that wait.
fn _drain<Io>(p: IoPool&, io: Io&) {
    loop {
        let k = p._oldest_queued();
        guard k >= 0 else { break; }
        p._finish(k, true, 0, IO_ADMIN_SHUTDOWN, "the loop is stopping");
    }
    for i in p.slots.len {
        let s .= p.slots[i];
        if s.phase == IO_IDLE {
            var o: u8[>..] = [];
            terminate(o);
            io.send(s.link, o);
            s.phase = IO_CLOSING;
            io.close(s.link);
        }
    }
}
