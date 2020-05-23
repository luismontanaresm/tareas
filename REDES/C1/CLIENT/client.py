import socket
import threading
import sys
import os
from datetime import datetime
from pathlib import Path
import json
import pickle
import sys
import sqlite3
requestID = "RequestID"


class FileDownload:
    def __init__(self, requestID, filename, host, port, message = str()):
        self.requestID = requestID
        self.filename = filename
        self.bytes_received = 0
        self.finished = False
        self.message = message
        self.host = host
        self.port = port

    def read_from_row(row):
        request_id = row[0]
        filename = row[1]
        bytes_received = int(row[2])
        finished = bool(int(row[3]))
        message = row[4]
        host = row[5]
        port = int(row[6])
        filedownload = FileDownload(request_id, filename, host, port , message)
        filedownload.bytes_received = bytes_received
        filedownload.finished = finished
        return filedownload
    
    def finished_as_int(self):
        return 1 if self.finished else 0

class Client:
    def __init__(self, downloads_dir="downloads", database="downloads.db"):
        directory = os.path.join( os.getcwd(), downloads_dir)
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.downloads_dir = downloads_dir
        self.downloads_queue = list()
        self.client_running = False
        self.downloading_file = True
        self.db_con = sqlite3.connect(database, check_same_thread=False)
        self.db_cursor = self.db_con.cursor()

        self.db_cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS downloads(
                id text PRIMARY KEY, 
                filename text, 
                bytes_received int, 
                finished int,
                message text, 
                host text, 
                port int)
            """
            )


    def insert_download_to_db(self, filedownload):
        self.db_cursor.execute(
            f"""
            INSERT INTO downloads VALUES(
                '{filedownload.requestID}',
                '{filedownload.filename}',
                {filedownload.bytes_received},
                {filedownload.finished_as_int()},
                '{filedownload.message}',
                '{filedownload.host}',
                {filedownload.port}
            )
            """
            )
        self.db_con.commit()
    
    def update_download_to_db(self, filedownload):
        self.db_cursor.execute(
            f"""
            UPDATE downloads SET
                bytes_received = {filedownload.bytes_received},
                finished = {filedownload.finished_as_int()}
            WHERE id = '{filedownload.requestID}'            
            """
            )
        self.db_con.commit()
    
    def read_database(self):
        self.db_cursor.execute(
            f"""
            SELECT * FROM downloads
            """
        )
        rows = self.db_cursor.fetchall()
        return rows
    
    def parse_url(self, url):
        scheme, host, port, path = str(), str(), str(), str()
        try:
            scheme = url[: url.index("//")]
            url  = url[url.index("//")+2:]
            host = url[: url.index(":")]
            port = url[url.index(":")+1:url.index("/")] if "/" in url else url[url.index(":")+1:]
            port = int(port)
            path = url[url.index("/"):] if "/" in url else "/"
        except:
            pass
        return scheme, host, port, path
    
    def unfinished_filedownloads_from_db(self):
        db_rows = list(self.read_database())
        db_filedownloads = [FileDownload.read_from_row(row) for  row in db_rows]
        unfinished_filedownloads = list( filter(lambda fd: not fd.finished, db_filedownloads) )
        return unfinished_filedownloads

    def add_unfinished_downloads_from_db(self):
        unfinished_downloads = self.unfinished_filedownloads_from_db()
        insertions = 0

        for unfinished_fd in unfinished_downloads:
            found = 0
            for fd in self.downloads_queue:
                if fd.requestID == unfinished_fd.requestID:
                    found = 1
            if not found:
                insertions += 1 
                self.downloads_queue.append(unfinished_fd)
        print("Se agregaron", insertions, "archivos a la cola de descargas")


    def request_id(self, host, port, message, bufsize):
        tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmp_sock.connect((host, port))
        tmp_sock.send(str.encode(message))
        rec = tmp_sock.recv(bufsize)
        str_rec = rec.decode("utf-8")
        given_request_id = str()
        if requestID in str_rec:   
            given_request_id_index = str_rec.find(requestID)
            given_request_id = str_rec[ given_request_id_index: ].split(" ")[1]
        
        tmp_sock.close()
        return given_request_id
    
    def request_file_by_chunks(self, filedownload, bufsize):
        self.downloading = True
        loop = True
        received_something = False
        host, port = filedownload.host, filedownload.port
        message, filename = filedownload.message, filedownload.filename
        
        tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmp_sock.connect((host, port))
        file = open(filename, "ab")
        file_bytes = None
        
        while loop and self.client_running:
            tmp_sock.send(str.encode(message))
            file_bytes = tmp_sock.recv(bufsize)
            file.write(file_bytes)
            if file_bytes: received_something = True
            filedownload.bytes_received += len(file_bytes)
            self.update_download_to_db(filedownload= filedownload)
            loop = file_bytes
        file.close()
        
        if received_something and not file_bytes:
            # Here, file has been downloaded completely
            # Client has received at least one byte and sometime it received and empty response
            filedownload.finished = True
            self.update_download_to_db(filedownload= filedownload)
            print(f"\u0398\t {filedownload.filename} ha completado su descarga")

        self.downloading = False
        return filedownload


    def new_download(self, url, filename):
        print(filename, "agregado a la cola de descargas")
        filedownload = None
        scheme, host, port, path = self.parse_url(url)
        if not( scheme and host and port and path ):
            return filedownload

        route_to_file = os.path.join(self.downloads_dir, filename)
        Path(route_to_file).touch()
        download_id = self.request_id(
            host = host, port = port,
            message=f"GET {path} {scheme.upper()}/1.1\r\n Host: {host}:{port}\r\n",
            bufsize= 256
        )
        if not download_id:
            return filedownload

        filedownload = FileDownload(
            download_id, route_to_file, host, port, 
            message= f"GET {path} {scheme.upper()}/1.1\r\n Host: {host}:{port}\r\n RequestID: {download_id}\r\n"
            )
        self.downloads_queue.append(filedownload)
        self.insert_download_to_db(filedownload)
        return filedownload
    
    def stop_downloads(self):
        
        #TODO stop async downloads
        self.client_running = False

    def resume_downloads(self):
        print("Resumiendo descargas")
        if self.client_running:
            return

        #TODO resume async downloads
        self.client_running = True
        
        while self.downloads_queue and self.client_running:
            filedownload = self.downloads_queue[0]
            self.request_file_by_chunks(filedownload, 2048)
            if filedownload.finished:
                self.downloads_queue.pop()
        
        if not self.downloads_queue:
            print("Se han completado todas las descargas")

    def resume_downloads_async(self):
        
        downloads_worker = threading.Thread(target= self.resume_downloads)
        downloads_worker.start()
    
    def show_queue(self):
        if not self.downloads_queue:
            print("No hay descargas pendientes")
        for d in self.downloads_queue:
            print(d.filename, d.bytes_received, "bytes.")
    
    def show_database(self):
        for row in self.read_database():
            print(row)

    def start(self):
        print(" \tLista de comandos aceptadas por el cliente\n")
        print("\u0394\tAgregar a la cola de descargas: -p new <url-remota> <nombre-archivo>. \n \t \tEjemplo: -p new http://127.0.0.1:8080/static/pride_and_prejudice.txt orgullo_y_prejuicio.txt")
        print("\u0394\tComenzar descargas: -p play")
        print("\u0394\tDetener descargas: -p stop")
        print("\u0394\tMostrar descargas pendientes: -p show")
        print("\u0394\tFinalizar el programa: -p poweroff")
        print("\n", "\u039E"*50)
        print("\n")
        print("Se encontraron", len(self.unfinished_filedownloads_from_db()), "descargas no finalizadas")
        print("\u0394\tAgregar a la cola descargas no finalizadas: -p resume-db")
        print("")
        appends_from_db = 1
        while True:
            instruction = input("Ingrese una instruccion: ")
            if instruction.startswith("-p play"):
                self.resume_downloads_async()
            if instruction.startswith("-p stop"):
                print("Deteniendo descargas")
                self.stop_downloads()
            if instruction.startswith("-p show"):
                self.show_queue()
            if instruction.startswith("-p new"):
                message = instruction.split(" ")
                url = message[2]
                filename = message[3]
                self.new_download(url, filename)
            if instruction.startswith("-p db"):
                self.show_database()
            if instruction.startswith("-p resume-db"):
                self.add_unfinished_downloads_from_db()
            if instruction.startswith("-p poweroff"):
                print("Deteniendo programa")
                self.stop_downloads()
                sys.exit(0)

if __name__ == "__main__":
    client = Client()
    client.start()
