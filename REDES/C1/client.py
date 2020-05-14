import socket
import sys
import os
from datetime import datetime

requestID = "RequestID"

class Client:
    def __init__(self, downloads_dir="downloads"):
        directory = os.path.join( os.getcwd(), downloads_dir)
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.downloads_dir = directory
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        self.logs = list()
        self.requests = list()
        self.connections = list()

    def connect(self, host, port):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self.sock.connect((host, port))
            self.logs.append(
                f"{now}\tconnection established to host:{host} in port:{port}")

            return 1
        except Exception:
            self.logs.append(
                f"ERROR\t{now}\tconnection refused to host:{host} in port:{port}")
            return 0
    
    def show_logs(self):
        for log in self.logs:
            print(log)

    def request(self, message, bufsize):
        self.sock.send(str.encode(message))
        given_request_id = str()        
        while True:
            rec = self.sock.recv(bufsize)
            if rec:
                rec = rec.decode("utf-8")
                if  requestID in rec and given_request_id:
                    print(rec)

                elif requestID in rec and not given_request_id:
                    print(rec)   
                    given_request_id_index = rec.find(requestID)
                    given_request_id = rec[ given_request_id_index: ].split(" ")[1]

                    new_message = message + f" {requestID}: {given_request_id}\r\n"
                    print(new_message)
                    self.sock.send(str.encode(new_message))
                else:
                    print(rec)
            else:
                break

    def request_file(self, host, port, bufsize, filename):
        self.connect(host, port)
        self.sock.send(str.encode("GET\n" + filename))
        rec = self.sock.recv(bufsize)
        if not rec:
            return "server closed connection"

        if rec[:2].decode("utf-8") != 'OK':
            return "server error: " + rec.decode("utf-8")
        rec = rec[:2]
        downloading_file = os.path.join(self.downloads_dir, filename)
        with open(downloading_file, 'wb') as output:
            if rec:
                output.write(rec)
            while True:
                rec = self.sock.recv(bufsize)
                if not rec:
                    break
                output.write(rec)

def parse_url(url):
    scheme = url[: url.index("//")]
    url  = url[url.index("//")+2:]
    host = url[: url.index(":")]
    port = url[url.index(":")+1:url.index("/")] if "/" in url else url[url.index(":")+1:]
    port = int(port)
    path = url[url.index("/"):] if "/" in url else "/"
    return scheme, host, port, path

if __name__ == "__main__":
    while True:
        client = Client()
        url  = input("Ingrese url: ")
        _, host, port, path = parse_url(url)
        client.connect(host= host, port= port)
        client.request(message= f"GET {path} HTTP/1.1\r\n Host: {host}:{port}\r\n", bufsize= 128)