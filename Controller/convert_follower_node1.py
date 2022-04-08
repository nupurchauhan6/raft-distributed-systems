import json
import socket
import traceback
import time
import threading
import random

leader = ""
port = 5555
is_leader_set = False


def listener(skt):
    while True:
        msg, addr = skt.recvfrom(1024)
        decoded_msg = json.loads(msg.decode('utf-8'))
        global leader
        leader = decoded_msg['value']
        global is_leader_set
        is_leader_set = True
        print(f"Message Received : {decoded_msg} From : {addr}")


def shutdown_leader(skt, nodes):
    leader_info(skt, nodes)
    global is_leader_set
    while(not is_leader_set):
        continue
    is_leader_set = False
    shutdown_node = leader
    request(skt, 'SHUTDOWN', [shutdown_node])


def convert_all_to_follower(skt, nodes):
    request(skt, 'CONVERT_FOLLOWER', nodes)


def convert_leader_to_follower(skt, nodes):
    leader_info(skt, nodes)
    global is_leader_set
    while(not is_leader_set):
        continue
    is_leader_set = False
    request(skt, 'CONVERT_FOLLOWER', [leader])


def convert_leader_to_follower(skt, nodes):
    leader_info(skt, nodes)
    global is_leader_set
    while(not is_leader_set):
        continue
    is_leader_set = False
    request(skt, 'CONVERT_FOLLOWER', [leader])


def convert_follower_to_follower(skt, nodes):
    convert_follower = random.choice(nodes)
    request(skt, 'CONVERT_FOLLOWER', [convert_follower])
    time.sleep(5)
    request(skt, 'CONVERT_FOLLOWER', [convert_follower])


def convert_leader_to_leader(skt, nodes):
    leader_info(skt, nodes)
    global is_leader_set
    while(not is_leader_set):
        continue
    is_leader_set = False
    request(skt, 'CONVERT_FOLLOWER', [leader])
    time.sleep(5)
    request(skt, 'CONVERT_FOLLOWER', [leader])


def leader_info(skt, nodes):
    request(skt, 'LEADER_INFO', nodes)


def shutdown_node(skt, nodes):
    shutdown_node = random.choice(nodes)
    request(skt, 'SHUTDOWN', [shutdown_node])


def timeout_node(skt, nodes):
    timeout_node = random.choice(nodes)
    request(skt, 'TIMEOUT', [timeout_node])


def timeout_leader(skt, nodes):
    leader_info(skt, nodes)
    global is_leader_set
    while(not is_leader_set):
        continue
    is_leader_set = False
    timeout_node = leader
    request(skt, 'TIMEOUT', [timeout_node])


def shutdown_node_convert_to_follower(skt, nodes):
    shutdown_node = random.choice(nodes)
    request(skt, 'SHUTDOWN', [shutdown_node])
    time.sleep(5)
    request(skt, 'CONVERT_FOLLOWER', [shutdown_node])


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

    # while run_test_case:
    #     select_test_case = random.randint(1, 7)
    #     testCases[select_test_case](skt, nodes)
    #     time.sleep(5)

    testCases[3](skt, nodes)
