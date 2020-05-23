from threading import Timer
import socket
import sys
import os
import uuid
from datetime import datetime
import time

REQUEST_ID = "RequestID"

class FileRequest:
    def __init__(self, requestID, filename):
        self.requestID = requestID
        self.filename = filename
        self.created_at = datetime.now()
        self.bytes_sent = 0


class Server:
    def __init__(self, staticfiles_dir = "static", time_to_live = 0):
        directory = os.path.join( os.getcwd(), staticfiles_dir)
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.staticfiles_dir = directory
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if time_to_live:
            ttl = Timer(time_to_live, self.stop)
            ttl.start()
        self.logs = list()
        self.requests = list()
        self.filerequests = dict()
        self.clients_map = dict()
    
    def uniqueID(self):
        id = str(uuid.uuid4())
        return id

    def get_value(self, message, key):
        value = message[message.find(key): ].split(" ")[1].replace("\r\n", "")
        return value

    def new_id_message(self, path):
        newID = self.uniqueID()
        filereq = FileRequest(newID, path)
        self.filerequests[newID] =  filereq
        message = bytes(f'{REQUEST_ID}: {newID}', 'utf-8')
        return message

    def response_builder(self, data, response_size = 2048):
        
        response = bytes()
        data = data.decode("utf-8")
        path = self.get_value(data, "GET")
        request_id = None
        if not REQUEST_ID in data:
            response = self.new_id_message(path)

        if REQUEST_ID in data:
            id = self.get_value(data, REQUEST_ID)
            request_id = id
            filerequest = self.filerequests.get(id)
            if filerequest:
                filepath = os.path.join(os.getcwd(), path[1:])
                if os.path.isfile(filepath):
                    file = open(filepath, "rb")
                    file.seek(filerequest.bytes_sent)
                    response = file.read(response_size)
                    file.close()
                    filerequest.bytes_sent+=response_size
                    self.filerequests[id] = filerequest
        return response, request_id


    def start(self, host, port, connections_limit):
        self.sock.bind((host, port))
        self.sock.listen(connections_limit)
        print(f"Server started listening on host {host} in port {port}\n")

        conn, addr = self.sock.accept()
        while True:
            try:
                data = conn.recv(2048)
                response, request_id = self.response_builder(data)
                if response:
                    time.sleep(0.01)
                    conn.sendall(response)
                if not response:
                    conn.close()
                    conn, addr = self.sock.accept()
                
            except:
                conn.close()
                conn, addr = self.sock.accept()
            
    def stop(self):
        print("Server has stop")
        self.sock.close()

if __name__ == "__main__":
    server = Server(staticfiles_dir= "static")
    host_port = sys.argv[1].split(":")
    host = host_port[0]
    port = int(host_port[1])
    server.start(host, port, 10)