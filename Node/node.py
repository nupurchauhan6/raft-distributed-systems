import socket
import json
import os
import time
import threading
import math
from Raft import RaftNode


def create_msg(sender, request, currentTerm):
    msg = {
        "sender_name": sender,
        "request": request,
        "term": currentTerm,
        "key": "",
        "value": ""
    }
    msg_bytes = json.dumps(msg).encode()
    return msg_bytes


def listener(skt, node: RaftNode, nodes):
    while True:
        msg, addr = skt.recvfrom(1024)
        decoded_msg = json.loads(msg.decode('utf-8'))
        print(f"Message Received : {decoded_msg} From : {addr}")

        if decoded_msg['request'] == "VOTE_REQUEST":
            if node.currentTerm < decoded_msg['term'] and node.votedFor == None:
                node.currentTerm += 1
                node.startTime = time.perf_counter()
                node.electionTimeout = node.getElectionTimeout()
                node.votedFor = decoded_msg['sender_name']
                msg_bytes = create_msg(sender, "VOTE_ACK", node.currentTerm)
                UDP_Socket.sendto(
                    msg_bytes, (decoded_msg['sender_name'], 5555))

        elif decoded_msg['request'] == "VOTE_ACK":
            node.voteCount += 1
            if node.voteCount > math.ceil((len(nodes)+1)/2.0):
                node.electionTimeout = node.getElectionTimeout()
                node.startTime = time.perf_counter()
                node.state = "LEADER"

        elif decoded_msg['request'] == "APPEND_RPC":
            node.electionTimeout = node.getElectionTimeout()
            node.startTime = time.perf_counter()
            if node.state == "CANDIDATE" and decoded_msg['term'] >= node.currentTerm:
                node.currentTerm = decoded_msg['term']
                node.state = "FOLLOWER"


if __name__ == "__main__":

    self_node = os.getenv('NODE_NAME')
    sender = self_node
    nodes = ["Node1", "Node2", "Node3"]
    targets = nodes
    targets.remove(self_node)
    node = RaftNode()
    print("Timeout for me is ", self_node, node.electionTimeout)

    UDP_Socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDP_Socket.bind((sender, 5555))

    thread = threading.Thread(target=listener, args=[UDP_Socket, node, nodes])
    thread.start()

    while(True):
        if node.state == "LEADER":
            if (node.startTime + node.electionTimeout) < time.perf_counter():
                node.electionTimeout = node.getElectionTimeout()
                node.startTime = time.perf_counter()
                for target in targets:
                    msg_bytes = create_msg(
                        sender, "APPEND_RPC", node.currentTerm)
                    UDP_Socket.sendto(msg_bytes, (target, 5555))

        if node.state == "FOLLOWER":
            if (node.startTime + node.electionTimeout) < time.perf_counter():
                node.state = "CANDIDATE"
                node.currentTerm += 1
                node.votedFor = self_node
                node.voteCount += 1

                for target in targets:
                    msg_bytes = create_msg(
                        sender, "VOTE_REQUEST", node.currentTerm)
                    UDP_Socket.sendto(msg_bytes, (target, 5555))
