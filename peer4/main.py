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
from pymongo import MongoClient
import base64
import struct

HOST = socket.gethostbyname(socket.gethostname())
PORT = 4004
FORMAT = "utf-8"
DISCONNECT_MESSAGE = "!Disconnected"
# PIECE_SIZE = 1024 * 16
PIECE_SIZE = 32

# uri = "mongodb+srv://tuduong05042003:TCNvGWABP04DAkBZ@natours-app-cluster.us9ca.mongodb.net/"
uri = "mongodb+srv://elfbe:elfbe123@cluster0.amkp2.mongodb.net/"
try:
    client = MongoClient(uri)
    print("Connected successfully!")
    
    db = client["mydatabase"]
    collection = db["mycollection"]
    
except Exception as e:
    print("Error:", e)
    client = None

def start_peer_server(peer_ip, peer_port):
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
        
        if request['type'] == 'UPDATE_PIECES_STATUS':
            info_hash = request['info_hash']
            pieces_status = request['pieces_status']

            try:
                with open('pieces_status.json', 'r') as f:
                    file_status_data = json.load(f)
            except FileNotFoundError:
                file_status_data = {}

            if not file_status_data.get(info_hash):
                file_status_data[info_hash] = {}

            if 'file_name' not in file_status_data[info_hash]:
                file_name = collection.find_one({"hashinfo": info_hash}).get("file_name", "Unknown")
                file_status_data[info_hash]['file_name'] = file_name

            file_status_data[info_hash]['pieces_status'] = pieces_status

            with open('pieces_status.json', 'w') as f:
                json.dump(file_status_data, f, indent=4)

            client_socket.sendall("OK".encode(FORMAT))
            
        elif(request['type'] == 'GET_FILE_STATUS'):
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

            if not data.get(info_hash):
                client_socket.sendall(struct.pack('!I', 0))
                return

            file_name = f"storage/{data[info_hash]['file_name']}"
            
            try:
                with open(file_name, "rb") as f:
                    for chunk_index in chunk_list:
                        f.seek(chunk_index * PIECE_SIZE)
                        data = f.read(PIECE_SIZE)
                        # Base64 encode binary data
                        chunk_data.append(base64.b64encode(data).decode('utf-8'))
            except FileNotFoundError:
                print(f"File {file_name} does not exist.")
                client_socket.sendall(struct.pack('!I', 0))
                return

            response['chunk_data'] = chunk_data
            json_data = json.dumps(response).encode(FORMAT)

            client_socket.sendall(struct.pack('!I', len(json_data)))
            client_socket.sendall(json_data)
        elif request['type'] == 'PING':
            response = {
                'type': 'PONG'
            }
            client_socket.sendall(json.dumps(response).encode('utf-8'))

def publish(file_path):
    try:
        with open(file_path, 'rb') as file:
            content = file.read(999999999)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return

    hash_func = hashlib.sha256()
    hash_func.update(content)

    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    new_data = {
        "hashinfo": hash_func.hexdigest(),
        "file_name": file_name,
        "file_size": file_size,
        "peer_ip": HOST,
        "peer_port": PORT
    }

    query = {"hashinfo": new_data["hashinfo"], "peer_port": new_data["peer_port"]}
    existing_doc = collection.find_one(query)

    if existing_doc:
        result = collection.update_one(query, {"$set": new_data})
        print(f"Updated existing document for file: {file_name}")
    else:
        result = collection.insert_one(new_data)
        print(f"Inserted new document for file: {file_name} with ID: {result.inserted_id}")

    print("Publish file successfully")


def fetch(infohash):
    tracker_server_database_data = list(collection.find({"hashinfo": infohash}))

    table = PrettyTable()
    table.field_names = ["Hash Info", "File Name", "File Size", "Peer IP", "Peer Port"]

    if tracker_server_database_data:
        for entry in tracker_server_database_data:
            table.add_row([
                entry.get("hashinfo", "N/A"),
                entry.get("file_name", "N/A"),
                entry.get("file_size", "N/A"),
                entry.get("peer_ip", "N/A"),
                entry.get("peer_port", "N/A")
            ])
        print(table)
    else:
        print(f"No entries found for infohash: {infohash}")
        
def fetch_all():
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
        
def assign_piece_status_to_peers(info_hash, num_of_pieces, peers_keep_file, file_name):
    peer_chunk_map = {i: [] for i in range(num_of_pieces)} 

    for i, (peer_ip, peer_port) in enumerate(peers_keep_file):
        for j in range(i, num_of_pieces, len(peers_keep_file)):
            peer_chunk_map[j].append((peer_ip, peer_port))

    for peer_ip, peer_port in peers_keep_file:
        pieces_status = [0] * num_of_pieces

        for chunk_index, peers in peer_chunk_map.items():
            if (peer_ip, peer_port) in peers:
                pieces_status[chunk_index] = 1

        request = {
            'type': 'UPDATE_PIECES_STATUS',
            'info_hash': info_hash,
            'pieces_status': pieces_status
        }

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((peer_ip, int(peer_port)))
                s.sendall(json.dumps(request).encode(FORMAT))
                response = s.recv(1024).decode(FORMAT)
                if response == "OK":
                    print(f"Updated pieces_status for peer {peer_ip}:{peer_port}")
                else:
                    print(f"Failed to update pieces_status for peer {peer_ip}:{peer_port}")
        except (socket.error, ConnectionRefusedError, TimeoutError) as e:
            print(f"Connection error with peer {peer_ip}:{peer_port}: {e}")

    try:
        with open('pieces_status.json', 'r') as f:
            file_status_data = json.load(f)
    except FileNotFoundError:
        file_status_data = {}

    if info_hash not in file_status_data:
        file_status_data[info_hash] = {
            'file_name': file_name,
            'pieces_status': [0] * num_of_pieces
        }

    for peer_ip, peer_port in peers_keep_file:
        pieces_status = [0] * num_of_pieces
        for chunk_index, peers in peer_chunk_map.items():
            if (peer_ip, peer_port) in peers:
                pieces_status[chunk_index] = 1

        file_status_data[info_hash]['pieces_status'] = pieces_status

    with open('pieces_status.json', 'w') as f:
        json.dump(file_status_data, f, indent=4)

    print("Pieces status and file name assigned and saved to pieces_status.json.")

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

        s.send(json.dumps(request).encode(FORMAT))

        data_length = struct.unpack('!I', s.recv(4))[0]

        if data_length == 0:
            print(f"Peer {peer_ip}:{peer_port} returned no data for chunks.")
            return

        response_data = b""
        while len(response_data) < data_length:
            packet = s.recv(4096)
            if not packet:
                break
            response_data += packet

        response = json.loads(response_data.decode(FORMAT))

        if response['type'] == 'FILE_CHUNK' and response['info_hash'] == info_hash:
            chunk_data = response['chunk_data']

            with open(file_path, "r+b") as f:
                for i, chunk in enumerate(chunk_data):
                    f.seek(chunk_list[i] * PIECE_SIZE)
                    f.write(base64.b64decode(chunk))
                    print(f"Chunk {chunk_list[i]} has been written into file")
        else:
            print("Received invalid response from peer")

def download(info_hash):
    peers_keep_file = []
    file_name = None
    file_size = None

    data = list(collection.find({"hashinfo": info_hash}))

    for entry in data:
        peers_keep_file.append((entry.get("peer_ip", "N/A"), entry.get("peer_port", "N/A")))
        if not file_name:
            file_name = entry.get("file_name", None)
            file_size = entry.get("file_size", None)
    
    file_path = f"storage/{file_name}"
    
    num_of_pieces = math.ceil(file_size / int(PIECE_SIZE))
    
    assign_piece_status_to_peers(info_hash, num_of_pieces, peers_keep_file, file_name)
    
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
        if len(params) > 1:
            fetch(params[1])
        else:
            fetch_all()
    elif(params[0] == 'download'):
        info_hashes = params[1:] 
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
