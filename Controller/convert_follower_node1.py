import json
import socket
import traceback
import time
import threading
import random
from test import *

leader = ''
port = 5555


def listener(skt):
    while True:
        msg, addr = skt.recvfrom(1024)
        decoded_msg = json.loads(msg.decode('utf-8'))
        leader = decoded_msg['value']
       
        print("Current Leader is:", leader)


def request(skt, name, nodes):
    msg = json.load(open("Message.json"))
    msg['sender_name'] = 'Controller'
    msg['request'] = name
    print(f"Request Created : {msg}")

    try:
        for target in nodes:
            skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
    except:
        print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")


if __name__ == "__main__":
    time.sleep(5)
    sender = "Controller"
    nodes = ["Node1", "Node2", "Node3"]

    skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    skt.bind((sender, port))

    threading.Thread(target=listener, args=[skt]).start()
    run_test_case = True

    testCases = {
        1: leader_info,
        2: convert_all_to_follower,
        3: convert_leader_to_follower,
        4: shutdown_node,
        5: shutdown_leader,
        6: timeout_node,
        7: timeout_leader,
    }

    while run_test_case:
        select_test_case = random.randint(1, 7)
        testCases[select_test_case](skt, nodes)
        time.sleep(5)
