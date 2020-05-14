from threading import Timer
import socket
import sys
import os
import uuid
from datetime import datetime

requestID = "RequestID"

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

    def start(self, host, port, connections_limit):
        self.sock.bind((host, port))
        self.sock.listen(connections_limit)
        print(f"Server started listening on host {host} in port {port}\n")

        conn, addr = self.sock.accept()
        while True:
            data = conn.recv(1024)
            if not data:
                continue
            
            data = data.decode("utf-8")
            path = data[ data.find("GET"): ].split(" ")[1]

            response = bytes()
            if requestID in data:
                id = data[ data.find(requestID): ].split(" ")[1].replace("\r\n", "")
                filerequest = self.filerequests.get(id)
                
                if filerequest:
                    filepath = os.path.join(os.getcwd(), path[1:])
                    print(f"Client {id} authenticated")
                    if not os.path.isfile(filepath):
                        #TODO send 404 error code
                        print("FILE DOESNOT EXIST")
                        pass
                    else: 
                        with open(filepath, "rb") as file:
                            while True:
                                data = file.read(1024)
                                if not data:
                                    break
                                conn.sendall(data)
                                filerequest.bytes_sent += len(data)
                else:
                    print("RECHAZAR CONEXION")
                    #TODO SEND 400 error code
            
            elif requestID not in data:
                newID = self.uniqueID()
                filereq = FileRequest(newID, path)
                self.filerequests[newID] =  filereq
                print("New client ID", newID)
                response = bytes(f'{requestID}: {newID}', 'utf-8')
            
            conn.send(response)
            
    def stop(self):
        print("Server has stop")
        self.sock.close()

if __name__ == "__main__":
    server = Server(staticfiles_dir= "static")
    host_port = sys.argv[1].split(":")
    host = host_port[0]
    port = int(host_port[1])
    server.start(host, port, 1)