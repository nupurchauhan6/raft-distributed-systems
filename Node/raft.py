import time
import random

initial_entry = {
    'term': -1,
    'key': "intial_entry",
    'value': "initial_entry"
}


class RaftNode:
    def __init__(self, state="FOLLOWER", currentTerm=0, votedFor=None, log=[initial_entry], voteCount=0):
        self.state = state
        self.currentTerm = currentTerm
        self.votedFor = votedFor
        self.log = log
        self.numLog = 1
        self.electionTimeout = self.getElectionTimeout()
        self.voteCount = voteCount
        self.startTime = time.perf_counter()
        self.currentLeader = ""
        self.shutdown = False
        self.heartbeatTimeout = self.getHeartbeatTimeout()
        self.commitIndex = 0
        self.lastApplied = 0
        self.nextIndex = []
        self.matchIndex = []
        self.commitCount = 0

    def getElectionTimeout(self):
        return random.randint(500, 5000)/1000.0

    def getHeartbeatTimeout(self):
        return 0.5

    def getLogIndex(self):
        return self.numLog - 1

    def getLogTerm(self):
        if self.numLog == 0:
            return 0
        entry = self.log[self.numLog-1]
        return entry["term"]
