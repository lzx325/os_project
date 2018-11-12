package raft

// TODO: remove comments
//TODO: remove debug msg
//TODO: remove getlog etc
//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	// "sort"
	"sync"
	"time"
	// "log"
)

import "bytes"
import "encoding/gob"

const ElectionTimoutLowerBound = 1000
const ElectionTimoutUpperBound = 1500
const ServerSleepTime = 10

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type State int

const (
	FOLLOWER  State = 0
	CANDIDATE State = 1
	LEADER    State = 2
)

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// common variables
	state           State
	currentTerm     int
	votedFor        int
	log             []LogEntry
	msgRecieved     bool
	commitIndex     int
	lastCommitIndex int
	applyCh         chan ApplyMsg

	// variables for Leader
	nextIndex  []int
	matchIndex []int
}

func randElectionTimeout() time.Duration {
	randTimeout := (ElectionTimoutLowerBound + rand.Intn(ElectionTimoutUpperBound-ElectionTimoutLowerBound))
	return time.Duration(randTimeout) * time.Millisecond
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isleader := (rf.state == LEADER)
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastCommitIndex)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.commitIndex)
	d.Decode(&rf.lastCommitIndex)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	CommitIndex  int
}

type AppendEntriesReply struct {
	Term      int
	NextIndex int
	Success   bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateCurrentTermNoLock(args.Term)

	DPrintf("Term[%d] -- Peer[%d] AppendEntries: Start... FROM [%d, %d]\n", rf.currentTerm, rf.me, args.LeaderId, args.Term)

	reply.Success = true
	reply.Term = rf.currentTerm
	reply.NextIndex = args.PrevLogIndex

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		if args.PrevLogIndex >= len(rf.log) {
			reply.NextIndex = len(rf.log)
		} else {
			index := args.PrevLogIndex
			targetTerm := rf.log[args.PrevLogIndex].Term
			for index > 0 && rf.log[index-1].Term == targetTerm {
				index--
			}
			reply.NextIndex = index
		}
		reply.Success = false
		return
	}

	conflictIndex := min(len(rf.log), len(args.Entries)+args.PrevLogIndex+1)
	for i := args.PrevLogIndex + 1; i < len(rf.log) && i < len(args.Entries)+args.PrevLogIndex+1; i++ {
		if rf.log[i].Term != args.Entries[i-args.PrevLogIndex-1].Term {
			conflictIndex = i
			break
		}
	}
	if conflictIndex != len(args.Entries)+args.PrevLogIndex+1 {
		rf.log = append(rf.log[0:conflictIndex], args.Entries[conflictIndex-(args.PrevLogIndex+1):]...)
	}

	if args.CommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.CommitIndex, len(rf.log)-1)
	}
	rf.msgRecieved = true
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateCurrentTermNoLock(args.Term)

	DPrintf("Term[%d] -- Peer[%d] RequestVote: Start...\n", rf.currentTerm, rf.me)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	lastIndex := len(rf.log) - 1
	if rf.log[lastIndex].Term > args.LastLogTerm ||
		(rf.log[lastIndex].Term == args.LastLogTerm && lastIndex > args.LastLogIndex) {
		return
	}
	if args.Term < rf.currentTerm {
		return
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	if rf.state == FOLLOWER {
		rf.msgRecieved = true
	}
	rf.persist()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	index = len(rf.log)
	term = rf.currentTerm
	isLeader = rf.state == LEADER

	if isLeader {
		DPrintf("Term[%d] -- Peer[%d] new command: index[%d] - [%d]\n", term, rf.me, index, command)
		rf.log = append(rf.log, LogEntry{term, command})
		rf.persist()
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	DPrintf("Term[%d] -- Peer[%d] ---------------- KILL ------------------\n", rf.currentTerm, rf.me)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.msgRecieved = false
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = []LogEntry{LogEntry{0, nil}}

	rf.lastCommitIndex = 0
	rf.commitIndex = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.readPersist(persister.ReadRaftState())

	go rf.checkElectionTimeout() // Candidate and Follower routine

	go rf.loopForAppendEntries() // Leader routine
	go rf.updateCommitIndex()    // Leader routine

	go rf.loopForApplyMsg() // Common routine

	DPrintf("Term[%d] -- Peer[%d] ---------------- START ------------------\n", rf.currentTerm, rf.me)
	return rf
}

func (rf *Raft) loopForAppendEntries() {
	for {
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			for idx := range rf.peers {
				if idx != rf.me {
					go func(server int) {
						nextIndex := rf.nextIndex[server]
						for nextIndex > 0 {
							rf.mu.Lock()
							entries := []LogEntry{}
							for ind := nextIndex; ind < len(rf.log); ind++ {
								entries = append(entries, rf.log[ind])
							}
							prevIndex := nextIndex - 1
							args := AppendEntriesArgs{rf.currentTerm, rf.me, prevIndex,
								rf.log[prevIndex].Term, entries, rf.commitIndex}
							reply := AppendEntriesReply{-1, -1, false}
							if rf.state != LEADER {
								rf.mu.Unlock()
								return
							}
							rf.mu.Unlock()

							ok := rf.sendAppendEntries(server, &args, &reply)
							if ok {
								rf.updateCurrentTerm(reply.Term)
								if reply.Success {
									rf.mu.Lock()
									rf.nextIndex[server] = nextIndex + len(args.Entries)
									rf.matchIndex[server] = rf.nextIndex[server] - 1
									rf.mu.Unlock()
									break
								} else {
									nextIndex = reply.NextIndex
								}
							} else {
								break
							}

						}
					}(idx)
				}
			}
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(ServerSleepTime * time.Millisecond)
	}
}

func (rf *Raft) handleElection(term int) {
	DPrintf("Term[%d] -- Peer[%d] handleElection(): begin...\n", term, rf.me)

	votedChan := make(chan bool)
	rf.mu.Lock()
	args := RequestVoteArgs{term, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}
	rf.mu.Unlock()
	for idx := range rf.peers {
		if idx != rf.me {
			go func(server int) {
				reply := RequestVoteReply{-1, false}
				ok := rf.sendRequestVote(server, &args, &reply)
				rf.updateCurrentTerm(reply.Term)
				select {
				case votedChan <- ok && reply.VoteGranted:
					DPrintf("Term[%d] -- Peer[%d] handleElection(): vote from [%d] - %v\n",
						term, rf.me, server, reply.VoteGranted)
				case <-time.After(time.Duration(ElectionTimoutUpperBound) * time.Millisecond):
				}
			}(idx)
		}
	}

	DPrintf("Term[%d] -- Peer[%d] handleElection(): waiting votes .\n", term, rf.me)
	total := len(rf.peers)
	receivedVotes := 1
	for i := 0; i < total-1; i++ {
		if <-votedChan {
			receivedVotes += 1
		}
		if receivedVotes >= (total+1)/2 {
			break
		}
	}

	DPrintf("Term[%d] -- Peer[%d] handleElection(): received votes [%d/%d].\n",
		term, rf.me, receivedVotes, total)

	if receivedVotes >= (total+1)/2 {
		rf.mu.Lock()
		if rf.state == CANDIDATE && term >= rf.currentTerm {
			rf.state = LEADER
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for idx := range rf.nextIndex {
				rf.nextIndex[idx] = len(rf.log)
			}
			DPrintf("Term[%d] -- Peer[%d] changed to Leader.\n", rf.currentTerm, rf.me)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) checkElectionTimeout() {
	for {
		time.Sleep(randElectionTimeout())
		rf.mu.Lock()
		if rf.state != LEADER && !rf.msgRecieved {
			rf.state = CANDIDATE
			rf.currentTerm++
			rf.votedFor = rf.me
			DPrintf("Term[%d] -- Peer[%d] changed to Candidate.\n", rf.currentTerm, rf.me)
			rf.persist()
			go rf.handleElection(rf.currentTerm)
		}
		rf.msgRecieved = false
		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCurrentTerm(term int) {
	rf.mu.Lock()
	rf.updateCurrentTermNoLock(term)
	rf.mu.Unlock()
}

func (rf *Raft) updateCurrentTermNoLock(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		DPrintf("Term[%d] -- Peer[%d] changed to Follower.\n", rf.currentTerm, rf.me)
	}
}

func (rf *Raft) updateCommitIndex() {
	for {
		rf.mu.Lock()
		if rf.state == LEADER {

			newCommitIndex := rf.commitIndex
			for newCommitIndex < len(rf.log) {
				numAppended := 0
				for idx := range rf.peers {
					if rf.matchIndex[idx] >= newCommitIndex+1 {
						numAppended++
					}
					if numAppended >= (len(rf.peers)-1)/2 {
						break
					}
				}
				if numAppended >= (len(rf.peers)-1)/2 {
					newCommitIndex++
				} else {
					break
				}
			}
			rf.commitIndex = newCommitIndex

		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(ServerSleepTime) * time.Millisecond) // an infinite loop must sleep for some time, otherwise deadlock
	}

}

func (rf *Raft) loopForApplyMsg() {
	for {
		rf.mu.Lock()
		for rf.lastCommitIndex < rf.commitIndex {
			msg := ApplyMsg{rf.lastCommitIndex + 1, rf.log[rf.lastCommitIndex+1].Command, false, []byte{}}
			DPrintf("Term[%d] -- Peer[%d] sending applyMsg: %v.  -- %v\n", rf.currentTerm, rf.me, msg, rf.log)
			rf.lastCommitIndex++
			rf.applyCh <- msg
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(ServerSleepTime) * time.Millisecond)
	}
}
