// The pool (pool.goose) on goose-io's in-memory provider, against a server scripted by time: a cleartext
// login, a query with a parameter and two rows, a second query on the same connection that fails, and
// a dial that is refused. The first query is made before the loop runs, and the answers come after
// the session is ready.
import std;
import hash from "github.com/kaitk/goose-hash@v0.0.4";
import goose_io from "../../../..";
import goose_io.memory;
import pg from "../../";
import pg.pool;
import check from "../lib/check/";

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

struct App { t1: pg::Ticket = pg::Ticket { .. }, t2: pg::Ticket = pg::Ticket { .. }, finished: i64 = 0, pool: pg::Pool }

// One request's query at every event, until it ends.
fn run(app: App&, io: goose_io_memory::Loop&, t: pg::Ticket&, name: const u8[:], sql: const u8[:]) {
    guard t.state != pg::TICKET_DONE else { return; }
    let n = pg::query(app.pool, t, io, 1000, sql, 3) { r => print(name, " row: ", pg::int(r, 0), " ", pg::text(r, 1)); };
    if n == pg::WAITS { return; }
    if n == pg::FAILS { print(name, " failed ", t.code[..], " ", app.pool.reason[..]); }
    else { print(name, ": ", n, " rows"); }
    app.finished++;
}

fn event(app: App&, io: goose_io_memory::Loop&, ev: goose_io::Event) {
    pg::pool_event(app.pool, io, ev);
    run(app, io, app.t1, "first", "SELECT id, name FROM items WHERE id < $1");
    if app.t1.state == pg::TICKET_DONE { run(app, io, app.t2, "second", "SELEC $1"); }
    if app.finished == 2 { io.stop(0); }
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

fn main() {
    print("-- two queries on one connection");
    var io = goose_io_memory::Loop { .. };
    let server = io.add_peer();
    io.peer_accept(server, "10.0.0.5:5432");
    io.peer_send(server, 1, check::message_of('R', check::auth(3, "")));
    io.peer_send(server, 5, login());
    io.peer_send(server, 20, first_answer());
    io.peer_send(server, 40, second_answer());
    var app = App { pool: pg::pool(config("10.0.0.5"), 2, 7) };
    run(app, io, app.t1, "first", "SELECT id, name FROM items WHERE id < $1");
    io.serve() { io, ev => event(app, io, ev); };
    let sent = io.peer_received(server);
    check::that("the password went in cleartext", find(sent[..], "p\0\0\0\x07pw\0") >= 0);
    check::that("the parameter went as text", find(sent[..], "\0\0\0\x013") >= 0);

    print("-- a server that refuses");
    var io2 = goose_io_memory::Loop { .. };
    io2.peer_refuse(io2.add_peer(), "10.0.0.6:5432");
    var app2 = App { finished: 1, pool: pg::pool(config("10.0.0.6"), 2, 7) };
    app2.t2.state = pg::TICKET_DONE;
    run(app2, io2, app2.t1, "refused", "SELECT 1");
    io2.serve() { io, ev => event(app2, io, ev); };
}
