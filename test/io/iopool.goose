// The pool on a goose-io loop (iopool.goose), run on goose-io's in-memory provider against a
// scripted server: it takes a password in cleartext, answers a query with two rows, answers a
// second query on the same connection with an error, and a dial to an address nobody takes
// fails the query that waited for it, once. The server's messages are sent by time, so the
// queries are submitted before the loop runs and the answers come after the session is ready.
import std;
import hash from "github.com/kaitk/goose-hash@v0.0.4";
import goose_io from "../../../..";
import goose_io.memory;
import pg from "../../";
import pg.iopool;
import check from "../lib/check/";

let TAG = 7;

// RowDescription for an int4 column "id" and a text column "name".
fn description() -> u8[>..] {
    var b: u8[>..] = [];
    pg::put16(b, 2);
    b.append("id\0");
    pg::put32(b, 0);
    pg::put16(b, 0);
    pg::put32(b, pg::OID_INT4);
    pg::put16(b, 4);
    pg::put32(b, -1);
    pg::put16(b, 0);
    b.append("name\0");
    pg::put32(b, 0);
    pg::put16(b, 0);
    pg::put32(b, pg::OID_TEXT);
    pg::put16(b, -1);
    pg::put32(b, -1);
    pg::put16(b, 0);
    var out: u8[>..] = [];
    check::message(out, 'T', b);
    return out;
}

fn data_row(id: const u8[:], name: const u8[:]) -> u8[>..] {
    var b: u8[>..] = [];
    pg::put16(b, 2);
    pg::put32(b, id.len);
    b.append(id);
    pg::put32(b, name.len);
    b.append(name);
    var out: u8[>..] = [];
    check::message(out, 'D', b);
    return out;
}

fn login() -> u8[>..] {
    var out: u8[>..] = [];
    check::message(out, 'R', check::auth(0, ""));
    check::message(out, 'S', "client_encoding\0UTF8\0");
    check::message(out, 'K', "\0\0\x01\x02\0\0\0\x07");
    check::message(out, 'Z', "I");
    return out;
}

fn first_answer() -> u8[>..] {
    var out: u8[>..] = [];
    check::message(out, '1', "");
    check::message(out, '2', "");
    out.append(description());
    out.append(data_row("1", "one"));
    out.append(data_row("2", "two"));
    check::message(out, 'C', "SELECT 2\0");
    check::message(out, 'Z', "I");
    return out;
}

fn second_answer() -> u8[>..] {
    var out: u8[>..] = [];
    check::message(out, 'E', "SERROR\0C42601\0Msyntax error\0\0");
    check::message(out, 'Z', "I");
    return out;
}

root struct App {
    pool: pg::IoPool,
    first: u32 = 0,
    second: u32 = 0,
    taken: i64 = 0,
    stop_after: i64 = 0,
}

fn show(app: App&) {
    loop {
        let d = app.pool.io_take();
        if !d.found { break; }
        var line: u8[>..] = [];
        format(line, "query ", d.id, ": ");
        if d.failed { format(line, "failed ", d.code[..], " ", d.message[..]); }
        else { format(line, d.rows, " rows"); }
        print(line);
        app.taken++;
    }
}

fn event(app: App&, io: goose_io_memory::Loop&, ev: goose_io::Event) {
    let own = app.pool.io_event(io, ev) { id, r =>
        print("query ", id, " row: ", pg::int(r, 0), " ", pg::text(r, 1));
    };
    show(app);
    if app.taken == 1 && app.second == 0 {
        app.second = pg::io_submit(app.pool, io, "SELEC", 1000);
        check::that("the second query is queued", app.second != 0);
    }
    if app.taken >= app.stop_after { io.stop(0); }
}

fn config(host: const u8[:]) -> pg::Config {
    var cfg = pg::Config { .. };
    cfg.host.append(host);
    cfg.user.append("u");
    cfg.password.append("pw");
    cfg.database.append("db");
    cfg.allow_cleartext = true;
    return cfg;
}

fn works() {
    print("-- two queries on one connection");
    var io = goose_io_memory::Loop { .. };
    let server = io.add_peer();
    io.peer_accept(server, "10.0.0.5:5432");
    io.peer_send(server, 1, check::message_of('R', check::auth(3, "")));
    io.peer_send(server, 5, login());
    io.peer_send(server, 20, first_answer());
    io.peer_send(server, 40, second_answer());
    var app = App { pool: pg::io_pool(config("10.0.0.5"), TAG, 2), stop_after: 2 };
    app.first = pg::io_submit(app.pool, io, "SELECT id, name FROM items WHERE id < $1", 1000) { w =>
        pg::param(w, 3);
    };
    check::that("the first query is queued", app.first != 0);
    io.serve() { io, ev => event(app, io, ev); };
    let sent = io.peer_received(server);
    check::that("the password went in cleartext", find(sent[..], "p\0\0\0\x07pw\0") >= 0);
    check::that("the first query's parameter went as text", find(sent[..], "\0\0\0\x013") >= 0);
    check::that("the pool opened one connection for two queries", app.pool.slots[1].phase == pg::IO_EMPTY);
}

fn refused() {
    print("-- a server that refuses");
    var io = goose_io_memory::Loop { .. };
    let server = io.add_peer();
    io.peer_refuse(server, "10.0.0.6:5432");
    var app = App { pool: pg::io_pool(config("10.0.0.6"), TAG, 2), stop_after: 1 };
    app.second = 1;
    let id = pg::io_submit(app.pool, io, "SELECT 1", 1000);
    check::that("the query is queued", id != 0);
    io.serve() { io, ev => event(app, io, ev); };
    check::that("nothing is left running", app.pool.live == 0);
}

fn refuses() {
    print("-- what io_submit refuses");
    var io = goose_io_memory::Loop { .. };
    var app = App { pool: pg::io_pool(config("10.0.0.7"), TAG, 1) };
    var long: u8[>..] = [];
    for i in pg::IO_SQL + 1 { long.push('x'); }
    check::that("SQL longer than IO_SQL", pg::io_submit(app.pool, io, long[..], 0) == 0);
    check::that("SQL with a NUL byte", pg::io_submit(app.pool, io, "SELECT\0 1", 0) == 0);
    check::that("parameters longer than IO_PARAMS", pg::io_submit(app.pool, io, "SELECT $1", 0) { w =>
        var big: u8[>..] = [];
        for i in pg::IO_PARAMS { big.push('x'); }
        pg::param(w, big[..]);
    } == 0);
    var n = 0;
    for i in pg::IO_REQUESTS + 5 { if pg::io_submit(app.pool, io, "SELECT 1", 0) != 0 { n++; } }
    check::eq("queries held at once", n, pg::IO_REQUESTS);
}

fn main() {
    works();
    refused();
    refuses();
}
