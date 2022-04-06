import socket
import json
import os
import time
import threading
import math
from raft import RaftNode
from constants import *


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


def vote_request(node: RaftNode, sender, term):
    if node.currentTerm < term and node.votedFor == None:
        node.currentTerm += 1
        node.startTime = time.perf_counter()
        node.electionTimeout = node.getElectionTimeout()
        node.votedFor = sender
        msg_bytes = create_msg(sender, VOTE_ACK, node.currentTerm)
        UDP_Socket.sendto(msg_bytes, (sender, 5555))


def vote_ack(node: RaftNode, nodes):
    node.voteCount += 1
    if node.voteCount > math.ceil((len(nodes)+1)/2.0):
        node.electionTimeout = node.getHeartbeatTimeout()
        node.startTime = time.perf_counter()
        node.state = LEADER


def append_rpc(node: RaftNode, term):
    node.electionTimeout = node.getElectionTimeout()
    node.startTime = time.perf_counter()
    if node.state == CANDIDATE and term >= node.currentTerm:
        node.currentTerm = term
        node.state = FOLLOWER


def convert_follower(node: RaftNode):
    node.state = FOLLOWER
    node.startTime = time.perf_counter()
    node.electionTimeout = node.getElectionTimeout()


def timeout(node: RaftNode):
    node.startTime = time.perf_counter()
    node.electionTimeout = 0


def shutdown():
    return


def leader_info(node: RaftNode):
    return


def listener(skt, node: RaftNode, nodes):
    while True:
        msg, addr = skt.recvfrom(1024)
        decoded_msg = json.loads(msg.decode('utf-8'))
        print(f"Message Received : {decoded_msg} From : {addr}")

        if decoded_msg['request'] == VOTE_REQUEST:
            vote_request(node, decoded_msg['sender_name'], decoded_msg['term'])

        elif decoded_msg['request'] == VOTE_ACK:
            vote_ack(node, nodes)

        elif decoded_msg['request'] == APPEND_RPC:
            append_rpc(node, decoded_msg['term'])

        elif decoded_msg['request'] == CONVERT_FOLLOWER:
            convert_follower(node)

        elif decoded_msg['request'] == TIMEOUT:
            timeout(node)

        elif decoded_msg['request'] == SHUTDOWN:
            return shutdown(node)

        elif decoded_msg['request'] == LEADER_INFO:
            leader_info(node)


if __name__ == "__main__":

    self_node = os.getenv('NODE_NAME')
    sender = self_node
    nodes = ["Node1", "Node2", "Node3"]
    targets = nodes
    targets.remove(self_node)
    node = RaftNode()

    UDP_Socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDP_Socket.bind((sender, 5555))

    thread = threading.Thread(target=listener, args=[UDP_Socket, node, nodes])
    thread.start()


    while(True):
        if node.state == LEADER:
            if (node.startTime + node.electionTimeout) < time.perf_counter():
                node.electionTimeout = node.getHeartbeatTimeout()
                node.startTime = time.perf_counter()
                for target in targets:
                    msg_bytes = create_msg(
                        sender, APPEND_RPC, node.currentTerm)
                    UDP_Socket.sendto(msg_bytes, (target, 5555))

        if node.state == FOLLOWER:
            if (node.startTime + node.electionTimeout) < time.perf_counter():
                node.state = CANDIDATE
                node.currentTerm += 1
                node.votedFor = self_node
                node.voteCount += 1

                for target in targets:
                    msg_bytes = create_msg(
                        sender, VOTE_REQUEST, node.currentTerm)
                    UDP_Socket.sendto(msg_bytes, (target, 5555))
