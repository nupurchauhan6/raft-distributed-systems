import socket
import json
import os
import time
import threading
import math

self_node = os.getenv('NODE_NAME')
sender = self_node
nodes = ["Node1", "Node2", "Node3"]
targets = nodes
targets.remove(self_node)

# Initial RAFT variables
id = ""
state = "FOLLOWER"
currentTerm = 0
votedFor = None
log = []
timeout = 0
heartbeat = 0
voters = []
electionTimeout = 0


def create_msg(request):
    msg = {
        "sender_name": sender,
        "request": request,
        "term": currentTerm,
        "key": "",
        "value": ""
    }
    msg_bytes = json.dumps(msg).encode()
    return msg_bytes


def listener(skt):
    while True:
        msg, addr = skt.recvfrom(1024)
        decoded_msg = json.loads(msg.decode('utf-8'))
        print(f"Message Received : {decoded_msg} From : {addr}")

        if decoded_msg['request'] == "VOTE_REQUEST":
            if currentTerm < decoded_msg['term'] and votedFor == None:
                votedFor = decoded_msg['sender_name']
                msg_bytes = create_msg("VOTE_ACK")
                UDP_Socket.sendto(
                    msg_bytes, (decoded_msg['sender_name'], 5555))

        elif decoded_msg['request'] == "VOTE_ACK":
            voters.append(decoded_msg['sender_name'])
            electionTimeout = time.perf_counter() + timeout
            if len(voters) > math.ceil((len(nodes)+1)/2.0):
                state = "LEADER"

        elif decoded_msg['request'] == "APPEND_RPC":
            if state == "CANDIDATE" and decoded_msg['term'] > currentTerm:
                state = "FOLLOWER"


if __name__ == "__main__":

    file = open('state.json', "r")
    data = json.loads(file.read())[self_node]
    id = data['id']
    timeout = (data['timeout']/1000)
    electionTimeout = time.perf_counter() + timeout

    UDP_Socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDP_Socket.bind((sender, 5555))

    thread = threading.Thread(target=listener, args=[UDP_Socket])
    thread.start()

    while(True):
        if state == "LEADER":
            for target in targets:
                msg_bytes = create_msg("APPEND_RPC")
                UDP_Socket.sendto(msg_bytes, (target, 5555))

        if state == "CANDIDATE":
            currentTerm = currentTerm + 1
            votedFor = self_node
            voters.append(self_node)

            for target in targets:
                msg_bytes = create_msg("VOTE_REQUEST")
                UDP_Socket.sendto(msg_bytes, (target, 5555))

        if state == "FOLLOWER":
            if time.perf_counter() > electionTimeout:
                state = "CANDIDATE"
