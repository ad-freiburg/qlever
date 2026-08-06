#!/usr/bin/env python3
"""Minimal logging HTTP forward proxy for the end-to-end tests: supports
absolute-form relaying (plain HTTP) and CONNECT tunneling (HTTPS). Logs every
request line to stdout and to the given log file, so that the test can verify
that a request actually went through the proxy. Usage: proxy.py PORT LOGFILE"""
import socket
import sys
import threading
from urllib.parse import urlsplit

LOG_LOCK = threading.Lock()
LOG_FILE = sys.argv[2] if len(sys.argv) > 2 else "proxy_log.txt"


def log(msg):
    with LOG_LOCK:
        print(msg, flush=True)
        with open(LOG_FILE, "a") as f:
            f.write(msg + "\n")


def pump(a, b):
    try:
        while True:
            data = a.recv(65536)
            if not data:
                break
            b.sendall(data)
    except OSError:
        pass
    finally:
        try:
            b.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def relay(client, upstream):
    t1 = threading.Thread(target=pump, args=(client, upstream))
    t2 = threading.Thread(target=pump, args=(upstream, client))
    t1.start()
    t2.start()
    t1.join()
    t2.join()


def handle(client, addr):
    try:
        client.settimeout(30)
        # Read the request header.
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = client.recv(65536)
            if not chunk:
                return
            data += chunk
        header, _, rest = data.partition(b"\r\n\r\n")
        request_line = header.split(b"\r\n")[0].decode("latin1")
        log(f"REQUEST: {request_line}")
        method, target, _version = request_line.split(" ", 2)

        if method == "CONNECT":
            host, _, port = target.rpartition(":")
            upstream = socket.create_connection((host.strip("[]"), int(port)), 30)
            client.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
            relay(client, upstream)
        else:
            # Absolute-form plain HTTP relay. A proper proxy client never sends
            # an origin-form target to a proxy, so reject that.
            parts = urlsplit(target)
            if not parts.scheme:
                log(f"REJECT (origin-form target): {target}")
                client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
                return
            host = parts.hostname
            port = parts.port or 80
            path = parts.path or "/"
            if parts.query:
                path += "?" + parts.query
            # Rewrite the request line to origin form for the upstream server.
            lines = header.split(b"\r\n")
            lines[0] = f"{method} {path} HTTP/1.1".encode("latin1")
            upstream = socket.create_connection((host, port), 30)
            upstream.sendall(b"\r\n".join(lines) + b"\r\n\r\n" + rest)
            relay(client, upstream)
    except Exception as e:
        log(f"ERROR: {e}")
    finally:
        client.close()


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 3128
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", port))
    server.listen(16)
    log(f"LISTENING on 127.0.0.1:{port}")
    while True:
        client, addr = server.accept()
        threading.Thread(target=handle, args=(client, addr), daemon=True).start()


if __name__ == "__main__":
    main()
