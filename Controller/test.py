import time

from convert_follower_node1 import *


def convert_all_to_follower(skt, nodes):
    request(skt, 'CONVERT_FOLLOWER', nodes)
    print("Leader all nodes to follower")


def convert_leader_to_follower(skt, nodes):
    leader_info(skt, nodes)
    global leader
    time.sleep(1)
    print("Controller ->", leader)
    request(skt, 'CONVERT_FOLLOWER', [leader])
    print("Leader Converted to follower")


def leader_info(skt, nodes):
    request(skt, 'LEADER_INFO', nodes)


def shutdown_node(skt, nodes):
    shutdown_node = random.choice(nodes)
    print("shutting Down Node->", shutdown_node, ".......")
    request(skt, 'SHUTDOWN', [shutdown_node])


def shutdown_leader(skt, nodes):
    leader_info(skt, nodes)
    time.sleep(1)
    shutdown_node = leader
    print("shutting Down Leader Node->", shutdown_node, ".......")
    request(skt, 'SHUTDOWN', [shutdown_node])


def timeout_node(skt, nodes):
    timeout_node = random.choice(nodes)
    print("Timing Out Node->", timeout_node, ".......")
    request(skt, 'TIMEOUT', [timeout_node])


def timeout_leader(skt, nodes):
    leader_info(skt, nodes)
    time.sleep(1)
    timeout_node = leader
    print("Timing Out Node->", timeout_node, ".......")
    request(skt, 'TIMEOUT', [timeout_node])
