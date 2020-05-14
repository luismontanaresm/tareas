import socket
import sys
import os

HOST = ""
PORT = 8050
BUF_SIZE = 4096

def recv_dl_file(conn):
    data = conn.recv(1024)
    if not data:
        print("Client finished")
        return None, None

    # Get command and filename
    try:
        cmd, down_file = data.decode("utf-8").split("\n")
    except:
        return None, "cannot parse client request"
    if cmd != 'GET':
        return None, "unknown command: " + cmd

    print(cmd, down_file)
    if not os.path.isfile(down_file):
        return None, 'no such file "%s"'%(down_file,)

    return down_file, None


def send_file(conn):
    down_file, err = recv_dl_file(conn)
    if err:
        print(err, file=sys.stderr)
        conn.send(bytes(err, 'utf-8'))
        return True

    if not down_file:
        return False # client all done

    # Tell client it is OK to receive file
    sent = conn.send(bytes('OK', 'utf-8'))

    total_sent = 0
    with open(down_file,'rb') as output:
        while True:
            data = output.read(BUF_SIZE)
            if not data:
                break
            conn.sendall(data)
            total_sent += len(data)

    print("finished sending", total_sent, "bytes")
    return True


sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((HOST, PORT))
sock.listen(1)
keep_going = 1
while keep_going:
    conn, addr = sock.accept()
    print("Connected by", str(addr))
    keep_going = send_file(conn)
    conn.close() # close clien connection

sock.close() # close listener
