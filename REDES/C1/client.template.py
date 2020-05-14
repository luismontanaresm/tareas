import socket
import sys
import os

HOST = "localhost"
PORT = 8050
BUF_SIZE = 4096
DOWNLOAD_DIR = "downloads"

def download_file(s, down_file):
    s.send(str.encode("GET\n" + down_file))
    rec = s.recv(BUF_SIZE)
    if not rec:
        return "server closed connection"

    if rec[:2].decode("utf-8") != 'OK':
        return "server error: " + rec.decode("utf-8")

    rec = rec[:2]
    if DOWNLOAD_DIR:
        down_file = os.path.join(DOWNLOAD_DIR, down_file)
    with open(down_file, 'wb') as output:
        if rec:
            output.write(rec)
        while True:
            rec = s.recv(BUF_SIZE)
            if not rec:
                break
            output.write(rec)

    print('Success!')
    return None

if DOWNLOAD_DIR and not os.path.isdir(DOWNLOAD_DIR):
    print('no such directory "%s"' % (DOWNLOAD_DIR,), file=sys.stderr)
    sys.exit(1)

while 1:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((HOST, PORT))
    except Exception as e:
        print("cannot connect to server:", e, file=sys.stderr)
        break

    file_name = input("\nFile to get: ")
    if not file_name:
        sock.close()
        break

    err = download_file(sock, file_name)
    if err:
        print(err, file=sys.stderr)
    sock.close()