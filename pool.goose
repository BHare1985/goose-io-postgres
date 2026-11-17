// A pool of connections on a goose-io loop, for a route that runs one
// query and answers from its rows (docs/architecture.md, "The pool"). Each
// loop keeps a Pool of its own. A request that finds no idle connection opens
// one, up to the pool's size, or polls for one at each of its calls. A program
// imports the module pg.pool, and declares goose-io in its root file.
//
//   import goose_io from "github.com/BHare1985/goose-io@<tag>";
//   import pg.pool;
//
//   var db = pg::pool(cfg, per_loop, TAG);     // before serve; TAG is the pool's tag
//   // in the loop's handler, once per event:
//   pg::pool_event(db, io, ev);
//   // then, for each request in flight, at every event, Tick included:
//   let n = pg::query(db, st.ticket, io, 5000, sql, lo, hi) { r =>
//       ... pg::int(r, 0), pg::text(r, 1) ...
//   };
//   if n == pg::WAITS { return; }              // called again at the next event
//   if n == pg::FAILS { ... }                  // st.ticket.code, db.reason
//
// The whole answer to the query is read into its connection's buffer,
// POOL_INPUT bytes, before the block sees the first row, so the block runs
// for every row within one call and the route answers in that call.
namespace pg;

import std;
import goose_io;
import hash from "github.com/kaitk/goose-hash@v0.0.4";

// Each connection's buffer: the most one query's answer may take, and so
// the longest message a pooled connection accepts.
let POOL_INPUT = 65536;
// The most requests that wait for a connection on one loop at once.
let POOL_WAITERS = 1024;

// The code of a request that found every connection busy and the queue full,
// Postgres's too_many_connections.
let TOO_MANY_CONNECTIONS = "53300";

// What query returns, besides a count of rows.
let WAITS = -1;   // the exchange waits: return from the handler
let FAILS = -2;   // no answer: Ticket.code and Pool.reason say why

// A connection's phases.
let SLOT_CLOSED: u8 = 0;
let SLOT_CONNECTING: u8 = 1;   // dialled; Opened not yet seen
let SLOT_STARTING: u8 = 2;     // the startup message sent; authenticating
let SLOT_OPEN: u8 = 3;

// A request's place with the pool, in the request's state.
let TICKET_NEW: u8 = 0;
let TICKET_QUEUED: u8 = 1;     // waits for a connection to be handed over
let TICKET_HAVE: u8 = 2;       // owns connection `conn`
let TICKET_DONE: u8 = 3;

struct Ticket {
    state: u8 = TICKET_NEW,
    conn: i64 = -1,
    // The first entry of the request's query once sent, and its Parse's
    // entry and statement when it sent one; -1 for none.
    first: i64 = -1,
    parse: i64 = -1,
    statement: i64 = -1,
    // The request's number in the pool, the limit on each wait, when the
    // current wait ends in the loop's clock (0: never), and the activity of
    // its connection that wait began at.
    key: u32 = 0,
    ms: i64 = 0,
    deadline: i64 = 0,
    seen: i64 = 0,
    // Why query returned FAILS: a SQLSTATE, the server's or one of the
    // facade's codes.
    code: u8[..5],
}

// A connection of the pool. owner is the key of the request that drives it,
// 0 for none: idle when open, free to connect when closed. A connection whose
// owner gave up, as after its time ran out, is adopted by the next request
// that finds none idle, which reads and drops the answers to queries it
// didn't send.
struct Slot {
    link: goose_io::Link = goose_io::Link { .. },
    phase: u8 = SLOT_CLOSED,
    // Opened has been delivered; Closed has been, or the buffer overflowed.
    up: bool = false,
    eof: bool = false,
    owner: u32 = 0,
    until: i64 = 0,
    // How many events the connection has had, so a wait is renewed only by its own.
    seen: i64 = 0,
    // Bit k: the pool's statement k is prepared on this connection.
    prepared: u64 = 0,
    // input[at..len] holds what has arrived and step hasn't read.
    at: i64 = 0,
    len: i64 = 0,
    c: Conn,
    input: u8[POOL_INPUT],
}

// A request waiting for a connection: its key, and when it gives up.
struct Waiter { key: u32 = 0, until: i64 = 0 }

// A loop's connections, with the settings to open them, the statements
// prepared on them by name, and the requests waiting for one. Build it in the
// thread that serves, with pool, and keep it last in that thread's state: the
// connections end it.
struct Pool {
    cfg: Config,
    tag: u16 = 1,
    next_key: u32 = 1,
    // Statement k is named "gp<k>"; its SQL is sql[ends[k - 1]..ends[k]].
    statements: i64 = 0,
    ends: i32[STATEMENTS],
    sql: u8[..STATEMENT_TEXT],
    // A ring of the requests waiting, from waiters[head].
    head: i64 = 0,
    waiting: i64 = 0,
    waiters: Waiter[POOL_WAITERS],
    // Why the last query that failed did.
    reason: u8[..120],
    slots: Slot[>..],
}

// A pool of size connections, at least one, to cfg's server, none open yet:
// each opens when a request needs it, and its links carry tag. A pooled
// connection takes messages up to POOL_INPUT - 1 bytes, or cfg.max_message if
// lower. The host is a numeric address, as goose-io's are: a name fails.
fn pool(cfg: Config, size: i64, tag: u16 = 1) -> Pool {
    var p = Pool { cfg: cfg, tag: tag, .. };
    p.cfg.max_message = min(cfg.max_message, POOL_INPUT - 1);
    for i in max(size, 1) { p.slots.push(Slot { c: Conn { .. }, .. }); }
    return p;
}

// Takes what the loop delivers for the pool's connections into their buffers:
// call it once for each event, before the requests' queries. True if the event
// was the pool's.
fn pool_event<Io>(p: Pool&, io: Io&, ev: goose_io::Event) -> bool {
    guard goose_io::is_link_event(ev) && goose_io::tag_of(ev) == p.tag else { return false; }
    let user = goose_io::user_of(ev);
    guard user >= 1 && user <= p.slots.len else { return true; }
    let s .= p.slots[user - 1];
    guard goose_io::same(s.link, goose_io::link_of(ev)) else { return true; }
    s.seen++;
    match ev {
        Opened e => { s.up = true; },
        Closed e => { s.eof = true; },
        Data e => {
            if s.len + e.bytes.len > POOL_INPUT { compact(s); }
            if s.len + e.bytes.len > POOL_INPUT {
                s.eof = true;
                io.reset(s.link);
            } else {
                for i in e.bytes.len { s.input[s.len + i] = e.bytes[i]; }
                s.len += e.bytes.len;
            }
        },
        _ => {},
    }
    return true;
}

// Ends each open session and closes every link, when the loop is stopping.
fn close<Io>(p: Pool&, io: Io&) {
    for k in p.slots.len {
        let s .= p.slots[k];
        if s.phase == SLOT_OPEN && !s.eof {
            var out: u8[>..] = [];
            terminate(out);
            io.send(s.link, out);
        }
        hang_up(s, io);
        s.owner = 0;
    }
}

// Runs sql on a connection of p for the request t belongs to, with up to
// eight parameters as the facade's query takes them, and calls F with each
// row, in the text format. Call it at the route's request and at every later
// event: it connects, waits for a connection, sends, and reads as each event
// allows, and returns WAITS while the exchange waits, the count of rows once
// F has seen them all, or FAILS. ms limits the whole query, from its first
// call; 0 is no limit. When the time runs out it gives up the connection and
// returns FAILS.
fn query<F>(p: Pool&, t: Ticket&, io, ms: i64, sql: const u8[:]) -> i64 {
    return pooled(p, t, io, ms, sql, F) { w => };
}

fn query<F>(p: Pool&, t: Ticket&, io, ms: i64, sql: const u8[:], p1) -> i64 {
    return pooled(p, t, io, ms, sql, F) { w => param(w, p1); };
}

fn query<F>(p: Pool&, t: Ticket&, io, ms: i64, sql: const u8[:], p1, p2) -> i64 {
    return pooled(p, t, io, ms, sql, F) { w =>
        param(w, p1);
        param(w, p2);
    };
}

fn query<F>(p: Pool&, t: Ticket&, io, ms: i64, sql: const u8[:], p1, p2, p3) -> i64 {
    return pooled(p, t, io, ms, sql, F) { w =>
        param(w, p1);
        param(w, p2);
        param(w, p3);
    };
}

fn query<F>(p: Pool&, t: Ticket&, io, ms: i64, sql: const u8[:], p1, p2, p3, p4) -> i64 {
    return pooled(p, t, io, ms, sql, F) { w =>
        param(w, p1);
        param(w, p2);
        param(w, p3);
        param(w, p4);
    };
}

fn query<F>(p: Pool&, t: Ticket&, io, ms: i64, sql: const u8[:], p1, p2, p3, p4, p5) -> i64 {
    return pooled(p, t, io, ms, sql, F) { w =>
        param(w, p1);
        param(w, p2);
        param(w, p3);
        param(w, p4);
        param(w, p5);
    };
}

fn query<F>(p: Pool&, t: Ticket&, io, ms: i64, sql: const u8[:], p1, p2, p3, p4, p5, p6) -> i64 {
    return pooled(p, t, io, ms, sql, F) { w =>
        param(w, p1);
        param(w, p2);
        param(w, p3);
        param(w, p4);
        param(w, p5);
        param(w, p6);
    };
}

fn query<F>(p: Pool&, t: Ticket&, io, ms: i64, sql: const u8[:], p1, p2, p3, p4, p5, p6, p7) -> i64 {
    return pooled(p, t, io, ms, sql, F) { w =>
        param(w, p1);
        param(w, p2);
        param(w, p3);
        param(w, p4);
        param(w, p5);
        param(w, p6);
        param(w, p7);
    };
}

fn query<F>(p: Pool&, t: Ticket&, io, ms: i64, sql: const u8[:], p1, p2, p3, p4, p5, p6, p7, p8) -> i64 {
    return pooled(p, t, io, ms, sql, F) { w =>
        param(w, p1);
        param(w, p2);
        param(w, p3);
        param(w, p4);
        param(w, p5);
        param(w, p6);
        param(w, p7);
        param(w, p8);
    };
}

// The Bind a query's parameters go into, for the block that writes them.
struct Binding { out: u8[>..]&, b: Bind }

fn param(w: Binding, v) { param(w.out, w.b, v); }

// query's work, with R the rows' block and W the writer of the parameters.
fn pooled<R, W>(p: Pool&, t: Ticket&, io, ms: i64, sql: const u8[:]) -> i64 {
    if (t.state == TICKET_QUEUED || t.state == TICKET_HAVE) && t.deadline != 0 && io.now_ms() >= t.deadline {
        if t.state == TICKET_HAVE && owns(p, t) {
            // A server that doesn't answer in time may not answer at all.
            hang_up(p.slots[t.conn], io);
            release(p, io, t.conn);
        }
        return fails(p, t, CONNECTION_FAILURE, "the database didn't answer in time");
    }
    if t.state == TICKET_QUEUED {
        var k = -1;
        for i in p.slots.len { if p.slots[i].owner == t.key { k = i; } }
        guard k >= 0 else { return WAITS; }
        t.state = TICKET_HAVE;
        t.conn = k;
        t.seen = p.slots[k].seen;
    }
    if t.state == TICKET_NEW {
        guard !has_nul(sql) else { return fails(p, t, CHARACTER_NOT_IN_REPERTOIRE, "the SQL holds a NUL byte"); }
        t.key = p.next_key;
        p.next_key = if p.next_key == 0xffffffff { 1 } else { p.next_key + 1 };
        t.ms = ms;
        t.deadline = if ms > 0 { io.now_ms() + ms } else { 0 };
        let r = acquire(p, t, io);
        guard r == 0 else { return r; }
    }
    guard t.state == TICKET_HAVE else { return fails(p, t, CONNECTION_DOES_NOT_EXIST, "the query has ended"); }
    return drive(p, t, io, ms, sql, R, W);
}

// Gives t a connection: an idle open one, then one without an owner to open,
// then one whose owner's time has run out, which goes to the queue's first
// request when there is one; or queues t, or fails when the queue is full.
// 0 when t has one.
fn acquire<Io>(p: Pool&, t: Ticket&, io: Io&) -> i64 {
    let now = io.now_ms();
    loop {
        var idle = -1;
        var unowned = -1;
        var abandoned = -1;
        for k in p.slots.len {
            let s .= p.slots[k];
            if s.owner == 0 {
                if s.phase == SLOT_OPEN {
                    idle = k;
                    break;
                }
                if unowned < 0 { unowned = k; }
            } else if abandoned < 0 && s.until != 0 && now >= s.until {
                abandoned = k;
            }
        }
        if idle < 0 && unowned < 0 && abandoned >= 0 && p.waiting > 0 {
            // Leaves it with a waiter, or with no owner if none still waits.
            release(p, io, abandoned);
            continue;
        }
        // An idle connection while requests wait is one that no one took yet:
        // it goes to the queue.
        if idle >= 0 && p.waiting > 0 {
            release(p, io, idle);
            if p.slots[idle].owner != 0 { continue; }
        }
        let k = if idle >= 0 { idle } else if unowned >= 0 { unowned } else { abandoned };
        guard k < 0 else {
            p.slots[k].owner = t.key;
            p.slots[k].until = t.deadline;
            t.state = TICKET_HAVE;
            t.conn = k;
            t.seen = p.slots[k].seen;
            return 0;
        }
        break;
    }
    if p.waiting == POOL_WAITERS { forget_gone_waiters(p, now); }
    guard p.waiting < POOL_WAITERS else {
        return fails(p, t, TOO_MANY_CONNECTIONS, "every connection is busy, and POOL_WAITERS requests wait");
    }
    p.waiters[(p.head + p.waiting) % POOL_WAITERS] = Waiter { key: t.key, until: t.deadline };
    p.waiting++;
    t.state = TICKET_QUEUED;
    return WAITS;
}

// Drops the waiters whose time has run out from p's queue, keeping the order.
fn forget_gone_waiters(p: Pool&, now: i64) {
    var kept = 0;
    for i in p.waiting {
        let w = p.waiters[(p.head + i) % POOL_WAITERS];
        if w.until == 0 || now < w.until {
            p.waiters[(p.head + kept) % POOL_WAITERS] = w;
            kept++;
        }
    }
    p.waiting = kept;
}

// Whether t still owns its connection.
fn owns(p: const Pool&, t: const Ticket&) -> bool {
    return t.conn >= 0 && t.conn < p.slots.len && p.slots[t.conn].owner == t.key;
}

// Hands connection k to the first request in the queue whose time hasn't run
// out, or leaves it idle.
fn release<Io>(p: Pool&, io: Io&, k: i64) {
    let s .= p.slots[k];
    s.owner = 0;
    s.until = 0;
    let now = io.now_ms();
    while p.waiting > 0 {
        let w = p.waiters[p.head];
        p.head = (p.head + 1) % POOL_WAITERS;
        p.waiting--;
        if w.until == 0 || now < w.until {
            s.owner = w.key;
            s.until = w.until;
            return;
        }
    }
}

// Waits for t's connection to have something new, with t's limit on the wait
// renewed by the connection's own events, not by the loop's others.
fn wait<Io>(p: Pool&, t: Ticket&, io: Io&) -> i64 {
    let s .= p.slots[t.conn];
    if s.seen != t.seen {
        t.seen = s.seen;
        if t.ms > 0 {
            t.deadline = io.now_ms() + t.ms;
            s.until = t.deadline;
        }
    }
    return WAITS;
}

// Drives t's connection as far as it can go without waiting: connects,
// authenticates, sends the query, and reads its answer.
fn drive<R, W>(p: Pool&, t: Ticket&, io, ms: i64, sql: const u8[:]) -> i64 {
    let s .= p.slots[t.conn];
    loop {
        if s.phase == SLOT_CLOSED {
            var where: u8[>..] = [];
            if find(p.cfg.host[..], ":") >= 0 { format(where, "[", p.cfg.host[..], "]:", p.cfg.port); }
            else { format(where, p.cfg.host[..], ":", p.cfg.port); }
            s.link = io.dial(where[..], p.tag, if ms > 0 { ms } else { 0 });
            io.set_user(s.link, (t.conn + 1) as u32);
            s.up = false;
            s.eof = false;
            s.phase = SLOT_CONNECTING;
            return wait(p, t, io);
        }
        if s.phase == SLOT_CONNECTING {
            if s.eof {
                return down(p, t, io, SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION, "can't connect to the database");
            }
            guard s.up else { return wait(p, t, io); }
            var nonce: u8[18] = [0; 18];
            hash::random_bytes(nonce);
            var o: u8[>..] = [];
            start(s.c, o, p.cfg, nonce);
            s.at = 0;
            s.len = 0;
            s.prepared = 0;
            guard s.c.state != BROKEN else { return down(p, t, io, SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION, s.c.reason); }
            guard io.send(s.link, o) == goose_io::SendStatus.Sent else { return down(p, t, io, CONNECTION_FAILURE, "sending to the server failed"); }
            s.phase = SLOT_STARTING;
            return wait(p, t, io);
        }
        let open = !s.eof;
        if s.phase == SLOT_OPEN && t.first < 0 {
            // A connection the server ended while it was idle is opened
            // again for this query.
            if !open || room(s.c) < 3 || !idle_ok(s) {
                hang_up(s, io);
                continue;
            }
            return send_query(p, t, io, sql, W);
        }
        // What arrived before the server closed may say why it did.
        if s.phase == SLOT_STARTING {
            var o: u8[>..] = [];
            let e, body = step(s.c, s.input[s.at..s.len], o);
            s.at += s.c.used;
            if o.len > 0 && io.send(s.link, o) != goose_io::SendStatus.Sent { return down(p, t, io, CONNECTION_FAILURE, "sending to the server failed"); }
            if e == READY {
                s.phase = SLOT_OPEN;
                continue;
            }
            if e == FAILED { return lost_slot(p, t, io, body); }
            if e == WANT_INPUT { return more(p, t, io, open); }
            continue;
        }
        return answer(p, t, io, open, R);
    }
}

// Writes t's query, a Parse first when the connection hasn't prepared sql,
// sends it, and waits for the answer.
fn send_query<W>(p: Pool&, t: Ticket&, io, sql: const u8[:]) -> i64 {
    let s .= p.slots[t.conn];
    var o: u8[>..] = [];
    var name: u8[..24] = [];
    let k = pool_statement(p, sql);
    if k >= 0 { format(name, "gp", k); }
    if k < 0 || (s.prepared & bit(k)) == 0 {
        t.parse = parse(s.c, o, name, sql);
        t.first = t.parse;
        if k >= 0 {
            // Set before the answer, so a query sent behind this one on the
            // connection binds the name rather than parse it again.
            s.prepared |= bit(k);
            t.statement = k;
        }
    }
    let b = bind(o, name);
    W(Binding { out: o, b: b });
    let q = execute(s.c, o, b);
    let y = sync(s.c, o);
    // A message longer than MAX_MESSAGE broke the connection: none is sent.
    guard s.c.state != BROKEN else { return down(p, t, io, CONNECTION_FAILURE, s.c.reason); }
    if t.first < 0 { t.first = if q >= 0 { q } else { y }; }
    // More than 65535 parameters: the Bind is dropped, and the query fails
    // with this code once the server has answered the rest.
    if q < 0 { t.code.append("54023"); }
    guard io.send(s.link, o) == goose_io::SendStatus.Sent else { return down(p, t, io, CONNECTION_FAILURE, "sending to the server failed"); }
    return wait(p, t, io);
}

fn bit(k: i64) -> u64 { (1 as u64) << k }

// The number of p's statement for sql, kept from now on if it's new, or -1
// when STATEMENTS or STATEMENT_TEXT has no room for it.
fn pool_statement(p: Pool&, sql: const u8[:]) -> i64 {
    var from = 0;
    for k in p.statements {
        let to = p.ends[k] as i64;
        if p.sql[from..to] == sql { return k; }
        from = to;
    }
    guard p.statements < STATEMENTS && p.sql.len + sql.len <= STATEMENT_TEXT else { return -1; }
    p.sql.append(sql);
    p.ends[p.statements] = p.sql.len as! i32;
    p.statements++;
    return p.statements - 1;
}

// Reads the answers on t's connection: those to queries sent before t's,
// dropped, then, once t's answer has arrived whole, each of its rows to R.
fn answer<R>(p: Pool&, t: Ticket&, io, open: bool) -> i64 {
    let s .= p.slots[t.conn];
    var count = 0;
    var failed = false;
    var code: u8[..5] = [];
    var message: u8[>..] = [];
    loop {
        guard s.c.count > 0 else { return down(p, t, io, CONNECTION_FAILURE, "the connection lost track of the query"); }
        if s.c.next - s.c.count >= t.first && !whole_answer(s) {
            guard s.at > 0 || s.len < POOL_INPUT else {
                return down(p, t, io, CONNECTION_FAILURE, "the answer is longer than POOL_INPUT");
            }
            return more(p, t, io, open);
        }
        var o: u8[>..] = [];
        let e, body = step(s.c, s.input[s.at..s.len], o);
        s.at += s.c.used;
        if e == WANT_INPUT { return more(p, t, io, open); }
        if e == FAILED { return lost_slot(p, t, io, body); }
        if e == ERROR && field(body, 'C') == INVALID_SQL_STATEMENT_NAME { s.prepared = 0; }
        guard s.c.seq >= t.first else { continue; }
        if e == ROW {
            R(Row { c: s.c, body: body });
        } else if e == DONE {
            count = rows(body);
        } else if e == ERROR {
            // 42P05 says the connection has the statement, which the mask may
            // have lost to a 26000 after a gone request's Parse failed.
            if s.c.seq == t.parse && t.statement >= 0 && field(body, 'C') != "42P05" {
                s.prepared &= ~bit(t.statement);
            }
            if !failed {
                failed = true;
                code.append(field(body, 'C')[..min(field(body, 'C').len, 5)]);
                message.append(field(body, 'M'));
            }
        } else if e == READY {
            compact(s);
            release(p, io, t.conn);
            if failed { return fails(p, t, code, message); }
            guard t.code.len == 0 else { return fails(p, t, "54023", "more than 65535 parameters, Postgres's limit"); }
            t.state = TICKET_DONE;
            return count;
        }
    }
}

// Reads what arrived on s with no query waiting, such as a notice, or the
// FATAL error of a server that ends the session: false for that.
fn idle_ok(s: Slot&) -> bool {
    guard s.c.count == 0 else { return true; }
    loop {
        var o: u8[>..] = [];
        let e, body = step(s.c, s.input[s.at..s.len], o);
        s.at += s.c.used;
        if e == FAILED { return false; }
        if e == WANT_INPUT { return true; }
    }
}

// Waits for more of the answer on t's connection, or, once the server has
// closed it, fails with the error it sent before, if any.
fn more(p: Pool&, t: Ticket&, io, open: bool) -> i64 {
    let s .= p.slots[t.conn];
    if open {
        compact(s);
        return wait(p, t, io);
    }
    loop {
        var o: u8[>..] = [];
        let e, body = step(s.c, s.input[s.at..s.len], o);
        s.at += s.c.used;
        if e == FAILED { return lost_slot(p, t, io, body); }
        if e == WANT_INPUT { return down(p, t, io, CONNECTION_FAILURE, "the server closed the connection, or receiving failed"); }
    }
}

// Whether s's input holds a whole ReadyForQuery: the end of the answer to
// the last query sent, when no other is waiting before it.
fn whole_answer(s: const Slot&) -> bool {
    var at = s.at;
    while at + 5 <= s.len {
        let size = get32(s.input, at + 1);
        guard size >= 4 && at + 1 + size <= s.len else { return false; }
        if s.input[at] == 'Z' { return true; }
        at += 1 + size;
    }
    return false;
}

// Moves what step hasn't read to the front of s's input.
fn compact(s: Slot&) {
    guard s.at > 0 else { return; }
    let keep = s.len - s.at;
    for i in keep { s.input[i] = s.input[s.at + i]; }
    s.at = 0;
    s.len = keep;
}

// Closes s's link, which the next request to take it dials again.
fn hang_up<Io>(s: Slot&, io: Io&) {
    if s.phase != SLOT_CLOSED && !s.eof { io.reset(s.link); }
    s.link = goose_io::Link { .. };
    s.phase = SLOT_CLOSED;
    s.up = false;
    s.eof = false;
    s.at = 0;
    s.len = 0;
    s.prepared = 0;
}

// t's connection failed: closes it, hands it on, and fails t.
fn down<Io>(p: Pool&, t: Ticket&, io: Io&, code: const u8[:], why: const u8[:]) -> i64 {
    hang_up(p.slots[t.conn], io);
    release(p, io, t.conn);
    return fails(p, t, code, why);
}

// t's connection broke in step: with the server's error, when it sent one.
fn lost_slot<Io>(p: Pool&, t: Ticket&, io: Io&, body: const u8[:]) -> i64 {
    var why: u8[>..] = [];
    let code = field(body, 'C');
    why.append(if code.len > 0 { field(body, 'M') } else { p.slots[t.conn].c.reason });
    return down(p, t, io, if code.len > 0 { code } else { CONNECTION_FAILURE }, why);
}

// Ends t with code and why.
fn fails(p: Pool&, t: Ticket&, code: const u8[:], why: const u8[:]) -> i64 {
    t.state = TICKET_DONE;
    t.code.clear();
    t.code.append(code[..min(code.len, 5)]);
    p.reason.clear();
    p.reason.append(why[..min(why.len, 120)]);
    return FAILS;
}
