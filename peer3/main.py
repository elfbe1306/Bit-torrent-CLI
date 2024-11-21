import os
import threading
import socket
import time
import hashlib
import json
from prettytable import PrettyTable
import math
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import random

HOST = '127.0.0.1'
PORT = 4002
FORMAT = "utf-8"
DISCONNECT_MESSAGE = "!Disconnected"
PIECE_SIZE = 2

def start_peer_server(peer_ip = "127.0.0.1", peer_port = "4002"):
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
        data = client_socket.recv(4096).decode(FORMAT)
        request = json.loads(data)
        
        if(request['type'] == 'GET_FILE_STATUS'):
            info_hash = request['info_hash']
            
            response = {
                'type': 'FILE_STATUS',
                'info_hash': info_hash,
                'pieces_status': []
            }
            
            with open('pieces_status.json', 'r') as f:
                data = json.load(f)
                
            if not data[info_hash]:
                client_socket.send(json.dumps(response).encode(FORMAT))
                return
            
            file_name = f"storage/{data[info_hash]['file_name']}"
            
            response = {
                'type': 'FILE_STATUS',
                'info_hash': info_hash,
                'pieces_status': data[info_hash]['pieces_status']
            }

            client_socket.send(json.dumps(response).encode(FORMAT))
            
        elif request['type'] == 'GET_FILE_CHUNK':
            info_hash = request['info_hash']
            chunk_list = request['chunk_list']
            chunk_data = []

            response = {
                'type': 'FILE_CHUNK',
                'info_hash': info_hash,
                'chunk_data': chunk_data
            }

            with open('pieces_status.json', 'r') as f:
                data = json.load(f)

            if not data[info_hash]:
                client_socket.send(json.dumps(response).encode('utf-8'))
                return
            file_name = f"storage/{data[info_hash]['file_name']}"
            
            try:
                with open(file_name, "rb") as f:
                    for chunk_index in chunk_list:
                        f.seek(chunk_index * PIECE_SIZE)
                        data = f.read(PIECE_SIZE)
                        chunk_data.append(data.decode('latin1'))
            except FileNotFoundError:
                print(f"File {file_name} does not exit.")
                client_socket.send(json.dumps(response).encode('utf-8'))
                return
            
            response['chunk_data'] = chunk_data

            client_socket.send(json.dumps(response).encode('utf-8'))
        elif request['type'] == 'PING':
            response = {
                'type': 'PONG'
            }
            client_socket.sendall(json.dumps(response).encode('utf-8'))

def publish(file_path):
    try:
        with open(file_path, 'rb') as file:
            content = file.read(65536)  # Read 64 KB
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return

    hash_func = hashlib.sha256()
    hash_func.update(content)

    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    # Prepare new data to be added
    new_data = {
        "hashinfo": hash_func.hexdigest(),
        "file_name": file_name,
        "file_size": file_size,
        "peer_ip": HOST,
        "peer_port": PORT
    }

    # File path for the database
    database_path = '../tracker_server_database.json'

    # Initialize existing data
    existing_data = []

    # Read the existing data from the JSON file
    if os.path.exists(database_path):
        try:
            with open(database_path, 'r') as f:
                data = json.load(f)
                # Ensure data is a list
                if isinstance(data, list):
                    existing_data = data
                else:
                    print("There are no data in JSON file. Overwriting with a new list.")
        except json.JSONDecodeError:
            print("Error reading the JSON file. Overwriting with a new list.")

    # Check if the hashinfo already exists and update the entry if found
    for i, entry in enumerate(existing_data):
        if entry.get("hashinfo") == new_data["hashinfo"] and entry.get("peer_port") == new_data["peer_port"]:
            existing_data[i] = new_data  # Replace the old entry with the new one
            break
    else:
        # If no matching hashinfo, append the new data
        existing_data.append(new_data)

    # Write the updated data back to the JSON file
    with open(database_path, 'w') as f:
        json.dump(existing_data, f, indent=4)

    print("Publish file successfully")


def fetch(infohash):
    database_path = '../tracker_server_database.json'
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
        if(entry.get("hashinfo") == infohash):
            found_matched_hash_info = True
            table.add_row([entry["hashinfo"], entry["file_name"], entry["file_size"], entry["peer_ip"], entry["peer_port"]])
    
    if found_matched_hash_info:
        print(table)
    else:
        print(f"No entries found for infohash: {infohash}")

def connect_to_peer_and_get_file_status(peer_ip, peer_port, info_hash):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            
            s.connect((peer_ip, peer_port))
            print(f"Connected to {peer_ip}:{peer_port}")
            
            request = {
                'type': 'GET_FILE_STATUS',
                'info_hash': info_hash
            }
            
            s.send(json.dumps(request).encode(FORMAT))
            
            response_data = s.recv(4096)
            response = json.loads(response_data.decode(FORMAT))
            if response['type'] == 'FILE_STATUS' and response['info_hash'] == info_hash:
                pieces_status = response['pieces_status']
                return peer_ip, peer_port, pieces_status
            else:
                return None, None, None
    
    except (socket.error, ConnectionRefusedError, TimeoutError) as e:
        print(f"Connection error: {e}")
        return None, None, None
    
def connect_to_peer_and_download_file_chunk(peer_ip, peer_port, info_hash, chunk_list, file_path):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((peer_ip, peer_port))
        print(f"Connected to {peer_ip}:{peer_port}")
        
        request = {
            'type': 'GET_FILE_CHUNK',
            'info_hash': info_hash,
            'chunk_list': chunk_list
        }

        s.send(json.dumps(request).encode('utf-8'))
        
        response_data = s.recv(4096)
        response = json.loads(response_data.decode('utf-8'))
        if response['type'] == 'FILE_CHUNK' and response['info_hash'] == info_hash:
            chunk_data = response['chunk_data']
            
            with open(file_path, "r+b") as f:  
                for i, chunk in enumerate(chunk_data):
                    f.seek(chunk_list[i] * PIECE_SIZE)
                    f.write(chunk.encode('latin1'))
                    print(f"Chunk {chunk_list[i]} has been written into file")
        else:
            print("Has been received invalid response from peer")

def download(info_hash):
    database_path = '../tracker_server_database.json'
    peers_keep_file = []
    file_name = None
    file_size = None

    try:
        with open(database_path, 'r') as f:
            data = json.load(f)

        for entry in data:
            if entry['hashinfo'] == info_hash:
                peers_keep_file.append((entry['peer_ip'], entry['peer_port']))
                if not file_name:
                    file_name = entry["file_name"]
                    file_size = entry["file_size"]
    except FileNotFoundError:
        print("Database not found.")
    
    file_path = f"storage/{file_name}"
    
    num_of_pieces = math.ceil(file_size / int(PIECE_SIZE))
    
    if not os.path.exists(file_path):
        with open(file_path, "wb") as f:
            pass
    
    peers_file_status = {}
    chunk_count = {}
    
    piece_status_lock = threading.Lock()
    chunk_count_lock = threading.Lock()
    piece_download_lock = threading.Lock()
    file_status_lock = threading.Lock()
    
    connection_queue = Queue()
    
    for address, port in peers_keep_file:
        if address != HOST or port != PORT:
            connection_queue.put((address, port))
    
    def get_file_status():
        while not connection_queue.empty():
            ip_address, port = connection_queue.get()
            try:
                peer_ip, peer_port, pieces_status = connect_to_peer_and_get_file_status(ip_address, port, info_hash)
                if len(pieces_status) != num_of_pieces:
                    continue
                
                with piece_status_lock:
                    peers_file_status[(peer_ip, peer_port)] = pieces_status
                
                with chunk_count_lock:
                    for chunk_index, has_chunk in enumerate(pieces_status):
                        if has_chunk:
                            if chunk_index not in chunk_count:
                                chunk_count[chunk_index] = 0
                            chunk_count[chunk_index] += 1        
            except:
                print(f"Error connecting to {ip_address}:{port}")
            connection_queue.task_done()
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        for _ in range(5):
            executor.submit(get_file_status)
    
    connection_queue.join()
    
    chunk_peers_map = {}
    for chunk_index in range(num_of_pieces):
        peers_with_chunk = [(peer, sum(status)) for peer, status in peers_file_status.items() if status[chunk_index]]
        if len(peers_with_chunk) > 0:
            chunk_peers_map[chunk_index] = peers_with_chunk
            random.shuffle(chunk_peers_map[chunk_index])

    chunk_queue = Queue()
    for chunk_index in chunk_peers_map.keys():
        chunk_queue.put(chunk_index)

    piece_has_been_downloaded = [0 for _ in range(num_of_pieces)]
    
    def download_chunk():
        while not chunk_queue.empty():
            chunk_index = chunk_queue.get()
            peers = chunk_peers_map.get(chunk_index, [])

            for (ip, port), _ in peers:
                with piece_download_lock:
                    if piece_has_been_downloaded[chunk_index] == 1:
                        continue
                
                try:
                    connect_to_peer_and_download_file_chunk(ip, port, info_hash, [chunk_index], file_path)

                    with piece_download_lock:
                        piece_has_been_downloaded[chunk_index] = 1
                    break
                except Exception as e:
                    print(f"Error downloading chunk {chunk_index} from {ip}:{port}: {e}")

            chunk_queue.task_done()

    with ThreadPoolExecutor(max_workers=5) as executor:
        for _ in range(5):
            executor.submit(download_chunk)

    chunk_queue.join()
    
    def update_file_status():
        with file_status_lock:
            try:
                with open('pieces_status.json', 'r') as f:
                    file_status_data = json.load(f)
                    if not file_status_data.get(info_hash):
                        file_status_data[info_hash] = {
                            'file_name': file_name,
                            'pieces_status': piece_has_been_downloaded
                        }
                    else:
                        file_status_data[info_hash]['pieces_status'] = piece_has_been_downloaded

                with open('pieces_status.json', 'w') as json_file:
                    json.dump(file_status_data, json_file, indent=4)
            except FileNotFoundError:
                print('File pieces_status.json does not exist')

    update_file_status()

    publish(file_path)
    print('Download and announce server successfully')
    
    
def process_input(cmd):
    params = cmd.split()
    if(params[0] == 'publish'):
        publish(params[1])
    elif(params[0] == 'fetch'):
        fetch(params[1])
    elif(params[0] == 'download'):
        info_hashes = params[1:]  # Accept multiple hashes
        for info_hash in info_hashes:
            download(info_hash)

if __name__ == "__main__":
    server_thread = threading.Thread(target=start_peer_server, args=(HOST, PORT))
    server_thread.start()

    time.sleep(1)
    while True:
        cmd = input('>>')
        if cmd == 'exit':
            break

        process_input(cmd)
