import socket
import threading
from prettytable import PrettyTable
import json
import os
import time

HOST = '127.0.0.1'
PORT = 5000

def start_peer_server(peer_ip='127.0.0.1', peer_port=5000):
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
    database_path = 'tracker_server_database.json'
    tracker_server_database_data = []
    
    if os.path.exists(database_path):
        try:
            with open(database_path, 'r') as f:
                tracker_server_database_data = json.load(f)  # Load all JSON data
        except json.JSONDecodeError:
            print("Error: The JSON file is not properly formatted.")
        except Exception as e:
            print(f"Error reading the JSON file: {e}")
    
    table = PrettyTable()
    table.field_names = ["Hash Info", "File Name","File Size", "Peer IP", "Peer Port"]

    found_matched_hash_info = False
    for entry in tracker_server_database_data:
        if(entry.get("hashinfo") == info_hash):
            found_matched_hash_info = True
            table.add_row([entry["hashinfo"], entry["file_name"], entry["file_size"], entry["peer_ip"], entry["peer_port"]])
    
    if found_matched_hash_info:
        print(table)
    else:
        print(f"No entries found for infohash: {info_hash}")

def process_input(cmd):
    params = cmd.split()
    if(params[0] == 'ping'):
        ping_client(params[1], params[2])
        
    elif(params[0] == 'discover'):
        get_peers_keep_file(params[1])
        
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