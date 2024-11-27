import socket
import threading
from prettytable import PrettyTable
import json
import os
import time
from pymongo import MongoClient

def get_real_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


HOST = get_real_ip()
PORT = 5000

uri = "mongodb+srv://elfbe:elfbe123@cluster0.amkp2.mongodb.net/"
try:
    client = MongoClient(uri, tlsAllowInvalidCertificates=True)
    print("Connected successfully!")
    
    db = client["mydatabase"]
    collection = db["mycollection"]
    
except Exception as e:
    print("Error:", e)
    client = None

def start_peer_server(peer_ip=socket.gethostbyname(socket.gethostname()), peer_port=5000):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((peer_ip, peer_port))
        server_socket.listen(5)
        print(f"Peer is listening at {peer_ip}:{peer_port}")
        print("Please type your command:\n")

        while True:
            client_socket, client_address = server_socket.accept()
            print(f"Connected to {client_address}")
            
            handle_request(client_socket)

def handle_request(client_socket):
    with client_socket:
        data = client_socket.recv(1024).decode('utf-8')
        request = json.loads(data)

        if request['type'] == 'PING':
            response = {
                'type': 'PONG',
            }

            client_socket.send(json.dumps(response).encode('utf-8'))

        elif request['type'] == 'DISCOVER':
            client_socket.send(json.dumps(response).encode('utf-8'))

def ping_client(peer_ip, peer_port):
    try:
        peer_port = int(peer_port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            
            s.connect((peer_ip, peer_port))
            print(f"Connected to {peer_ip}:{peer_port}")
            
            request = {
                'type': 'PING',
            }

            s.sendall(json.dumps(request).encode('utf-8'))
            
            response_data = s.recv(4096)
            response = json.loads(response_data.decode('utf-8'))
            if response['type'] == 'PONG':
                print('Client is working')
            else:
                print('Client is not working')
    except (socket.error, ConnectionRefusedError, TimeoutError) as e:
        print('Client is not working')

def get_peers_keep_file(info_hash):
    tracker_server_database_data = list(collection.find({"hashinfo": info_hash}))

    table = PrettyTable()
    table.field_names = ["Hash Info", "File Name", "File Size", "Peer IP", "Peer Port"]

    found_matched_hash_info = False

    for entry in tracker_server_database_data:
        found_matched_hash_info = True
        table.add_row([
            entry.get("hashinfo", "N/A"),
            entry.get("file_name", "N/A"),
            entry.get("file_size", "N/A"),
            entry.get("peer_ip", "N/A"),
            entry.get("peer_port", "N/A")
        ])

    if found_matched_hash_info:
        print(table)
    else:
        print(f"No entries found for infohash: {info_hash}")
        
def discover_all():
    all_files_data = list(collection.find())

    table = PrettyTable()
    table.field_names = ["Hash Info", "File Name", "File Size", "Peer IP", "Peer Port"]

    if all_files_data:
        for entry in all_files_data:
            table.add_row([
                entry.get("hashinfo", "N/A"),
                entry.get("file_name", "N/A"),
                entry.get("file_size", "N/A"),
                entry.get("peer_ip", "N/A"),
                entry.get("peer_port", "N/A")
            ])
        print(table)
    else:
        print("No files found in the tracker server.")

def process_input(cmd):
    params = cmd.split()
    if(params[0] == 'ping'):
        ping_client(params[1], params[2])
        
    elif(params[0] == 'discover'):
        if len(params) > 1:
            get_peers_keep_file(params[1])
        else:
            discover_all()
          
    else:
        print('Invalid command')

if __name__ == "__main__":
	try:
		server_thread = threading.Thread(target=start_peer_server, args=(HOST, PORT))
		server_thread.start()

		time.sleep(1)
		while True:
			cmd = input('>>')
			if cmd == 'exit':
				break
			process_input(cmd)


	except KeyboardInterrupt:
		print('\nMessenger stopped by user')
	finally:
		print("Cleanup done.")