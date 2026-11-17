// What socket.goose gives the pool besides goose-http's calls, for a pool on goose-io (httpshim.goose): the address
// it dials and the message of a connect that failed. The module pg.socketshim stands where pg.socket does for
// pool.goose, and needs no goose-http.
namespace pg;

import std;
import .httpshim;

// What a Config's route dials: its host, or unix: and the server's socket in the directory a host that starts with /
// names. cfg is a Config; untyped, so that `goose check` of this file alone takes it.
fn socket_address(cfg) -> u8[>..] {
    var where: u8[>..] = [];
    if cfg.host.len > 0 && cfg.host[0] == '/' {
        format(where, "unix:", cfg.host, if cfg.host[cfg.host.len - 1] == '/' { "" } else { "/" }, ".s.PGSQL.", cfg.port);
    } else {
        where.append(cfg.host);
    }
    return where;
}

// Why a connect to where and port failed, for an Error's message.
fn cant_connect(where: const u8[:], port: i64, e: httpshim::Error) -> u8[] {
    var message: u8[>..] = [];
    format(message, "can't connect to ", where);
    if !starts_with(where, "unix:") { format(message, " port ", port); }
    format(message, ": ", e.call, " ");
    if e.code == httpshim::ECONNREFUSED { message.append("ECONNREFUSED"); }
    else if e.code == httpshim::ETIMEDOUT { message.append("ETIMEDOUT"); }
    else if e.code == httpshim::ENOENT { message.append("ENOENT"); }
    else { format(message, "error ", e.code); }
    return str(message);
}
