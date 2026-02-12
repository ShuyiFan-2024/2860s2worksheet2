#!/usr/bin/env python3
import argparse, os, socket, struct, sys

CHUNK = 8192

def send_all(s, data: bytes):
    mv = memoryview(data)
    while mv:
        n = s.send(mv)
        if n <= 0:
            raise OSError
        mv = mv[n:]

def recv_exact(s, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        part = s.recv(n - len(buf))
        if not part:
            raise OSError
        buf += part
    return bytes(buf)

def send_u16(s, x): send_all(s, struct.pack("!H", x))
def recv_u16(s): return struct.unpack("!H", recv_exact(s, 2))[0]
def send_u64(s, x): send_all(s, struct.pack("!Q", x))
def recv_u64(s): return struct.unpack("!Q", recv_exact(s, 8))[0]

def server(port, outdir, ipv6):
    family = socket.AF_INET6 if ipv6 else socket.AF_INET
    bind_addr = ("::", port) if ipv6 else ("0.0.0.0", port)

    with socket.socket(family, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(bind_addr)
        srv.listen(5)

        os.makedirs(outdir, exist_ok=True)

        while True:
            c, _ = srv.accept()
            with c:
                try:
                    n = recv_u16(c)
                    filename = recv_exact(c, n).decode("utf-8")
                    outpath = os.path.join(outdir, filename + "-received")

                    if os.path.exists(outpath):
                        send_all(c, b"NO")
                        continue
                    send_all(c, b"OK")

                    total = recv_u64(c)
                    left = total
                    with open(outpath, "wb") as f:
                        while left > 0:
                            k = CHUNK if left > CHUNK else left
                            data = recv_exact(c, k)
                            f.write(data)
                            left -= len(data)

                    send_all(c, b"ACK")
                except:
                    continue

def client(connect, port, filepath, ipv6):
    try:
        filename = os.path.basename(filepath)
        family = socket.AF_INET6 if ipv6 else socket.AF_INET

        with open(filepath, "rb") as f, socket.socket(family, socket.SOCK_STREAM) as s:
            if ipv6:
                s.connect((connect, port, 0, 0))
            else:
                s.connect((connect, port))

            name = filename.encode("utf-8")
            send_u16(s, len(name))
            send_all(s, name)

            resp = recv_exact(s, 2)
            if resp == b"NO":
                return 1
            if resp != b"OK":
                return 255

            size = os.fstat(f.fileno()).st_size
            send_u64(s, size)

            while True:
                chunk = f.read(CHUNK)
                if not chunk:
                    break
                send_all(s, chunk)

            ack = recv_exact(s, 3)
            return 0 if ack == b"ACK" else 255
    except:
        return 255

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", action="store_true")
    ap.add_argument("--client", action="store_true")
    ap.add_argument("--ipv6", action="store_true")
    ap.add_argument("--port", type=int, default=9090)
    ap.add_argument("--outdir", default="./")
    ap.add_argument("--connect", default="127.0.0.1")
    ap.add_argument("--file")
    args = ap.parse_args()

    if args.server:
        server(args.port, args.outdir, args.ipv6)
    else:
        if not args.file:
            sys.exit(255)
        sys.exit(client(args.connect, args.port, args.file, args.ipv6))

if __name__ == "__main__":
    main()