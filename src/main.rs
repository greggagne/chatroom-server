//! Chatroom server.
//!
//! Listens on port 3500 and hands each connection to its own thread.
//! Each thread reads protocol lines, prints them, and on successful `JOIN`
//! adds the username to a shared in-room list, and removes it on `EXIT` or
//! disconnect. Commands are not fully handled yet: the server does not send
//! status codes or route messages.

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

/// Port the chatroom protocol listens on.
const PORT: u16 = 3500;

/// Stop reading a line after this many bytes if no newline has arrived.
const MAX_LINE_LEN: usize = 8192;

const MAX_USERNAME_LEN: usize = 16;

type RoomMembers = Arc<Mutex<HashSet<String>>>;

fn main() {
    let listener = match TcpListener::bind(("0.0.0.0", PORT)) {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("failed to bind port {PORT}: {e}");
            return;
        }
    };

    let room: RoomMembers = Arc::new(Mutex::new(HashSet::new()));

    println!("chatroom server listening on 0.0.0.0:{PORT}");

    for stream in listener.incoming() {
        match stream {
            Ok(client) => {
                let room = Arc::clone(&room);
                thread::spawn(move || handle_client(client, room));
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
}

/// Per-connection state for membership in the chatroom.
struct ClientSession {
    in_room: bool,
    username: Option<String>,
}

/// Read lines from one client and print them.
///
/// A line ends at the two bytes `\r\n`. A bare `\n` does not end a line.
/// If 8192 bytes arrive with no newline, the connection is closed.
/// The socket closes when `client` is dropped at the end of this thread.
fn handle_client(mut client: TcpStream, room: RoomMembers) {
    let peer = peer_label(&client);
    println!("{peer} connected");

    let mut session = ClientSession {
        in_room: false,
        username: None,
    };

    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 1024];

    loop {
        if let Some(line) = take_next_line(&mut buf) {
            if handle_line(&peer, &line, &room, &mut session) {
                return;
            }
            continue;
        }

        if buf.len() >= MAX_LINE_LEN {
            println!("{peer} sent {MAX_LINE_LEN} bytes with no newline; closing");
            clear_membership(&room, &mut session);
            return;
        }

        let room_left = MAX_LINE_LEN - buf.len();
        let to_read = room_left.min(chunk.len());
        match client.read(&mut chunk[..to_read]) {
            Ok(0) => break,
            Ok(n) => {
                output_read(&peer, &chunk[..n]);
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(e) => {
                eprintln!("{peer}: {e}");
                break;
            }
        }
    }

    if !buf.is_empty() {
        let text = String::from_utf8_lossy(&buf);
        println!("{peer} closed with an incomplete line: {text}");
    }
    clear_membership(&room, &mut session);
    println!("{peer} disconnected");
}

/// Returns `true` when the connection should close (e.g. after `EXIT`).
fn handle_line(
    peer: &str,
    line: &[u8],
    room: &RoomMembers,
    session: &mut ClientSession,
) -> bool {
    let Ok(text) = std::str::from_utf8(line) else {
        println!("{peer}: invalid UTF-8 line (ignored for membership)");
        return false;
    };

    if text == "EXIT" {
        handle_exit(peer, room, session);
        return true;
    }

    if let Some(username) = parse_join_username(text) {
        attempt_join(peer, username, room, session);
        print_members(room);
    }

    false
}

/// `Some(username)` if the line is a `JOIN` command (username may be empty).
fn parse_join_username(line: &str) -> Option<&str> {
    if line == "JOIN" {
        return Some("");
    }
    line.strip_prefix("JOIN ")
}

fn attempt_join(peer: &str, username: &str, room: &RoomMembers, session: &mut ClientSession) {
    if session.in_room {
        println!("{peer}: JOIN while already in room (ignored)");
        return;
    }

    if username.is_empty() {
        println!("{peer}: JOIN with empty username (not added)");
        return;
    }
    if username.chars().count() > MAX_USERNAME_LEN {
        println!("{peer}: JOIN username too long (not added)");
        return;
    }
    if !username.chars().all(|c| c.is_ascii_alphanumeric()) {
        println!("{peer}: JOIN username has illegal characters (not added)");
        return;
    }

    let added = {
        let mut members = room.lock().unwrap();
        if members.contains(username) {
            false
        } else {
            members.insert(username.to_string());
            true
        }
    };

    if added {
        session.in_room = true;
        session.username = Some(username.to_string());
        println!("{peer}: {username} joined");
    } else {
        println!("{peer}: JOIN {username} — username already in room (not added)");
    }
}

fn handle_exit(peer: &str, room: &RoomMembers, session: &mut ClientSession) {
    if let Some(name) = remove_session_from_room(room, session) {
        println!("{peer}: {name} exited");
    }
    print_members(room);
}

/// Drops this connection from the shared member set. Returns the username removed.
fn remove_session_from_room(room: &RoomMembers, session: &mut ClientSession) -> Option<String> {
    if !session.in_room {
        return None;
    }
    session.in_room = false;
    let username = session.username.take()?;
    room.lock().unwrap().remove(&username);
    Some(username)
}

/// Remove this connection from the room without printing the member list.
fn clear_membership(room: &RoomMembers, session: &mut ClientSession) {
    let _ = remove_session_from_room(room, session);
}

fn print_members(room: &RoomMembers) {
    let members = room.lock().unwrap();
    if members.is_empty() {
        println!("chatroom members: (none)");
        return;
    }
    let mut names: Vec<_> = members.iter().cloned().collect();
    names.sort();
    println!("chatroom members: {}", names.join(", "));
}

fn peer_label(client: &TcpStream) -> String {
    match client.peer_addr() {
        Ok(addr) => format_addr(addr),
        Err(_) => "unknown".to_string(),
    }
}

fn format_addr(addr: SocketAddr) -> String {
    format!("{}:{}", addr.ip(), addr.port())
}

/// Takes one complete line from `buf`, if present.
///
/// Protocol lines end with `\r\n`. A lone `\n` (e.g. from `nc` on Enter) is
/// also accepted so `JOIN` and `EXIT` work in simple tests.
fn take_next_line(buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    if let Some(cr_at) = buf.windows(2).position(|pair| pair == b"\r\n") {
        let line = buf[..cr_at].to_vec();
        buf.drain(..cr_at + 2);
        return Some(line);
    }
    if let Some(nl_at) = buf.iter().position(|&b| b == b'\n') {
        let mut line = buf[..nl_at].to_vec();
        buf.drain(..nl_at + 1);
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        return Some(line);
    }
    None
}

fn output_read(peer: &str, bytes: &[u8]) {
    let mut out = std::io::stdout().lock();
    let _ = write!(out, "{peer}: ");
    let _ = out.write_all(bytes);
    let _ = out.flush();
}