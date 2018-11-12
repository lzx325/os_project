package raft

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

import "sync"
import "labrpc"
import "time"
import "log"
import "math/rand"
import "bytes"
import "encoding/gob"
import "strconv"
import "fmt"

const ELECTION_TIMEOUT_LOWER_BOUND int = 500
const ELECTION_TIMEOUT_UPPER_BOUND int = 1000
const LEADER_SLEEP_INTERVAL int = 50

func randRange(lower, upper int) int {
	return rand.Intn(upper-lower) + lower
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a, b time.Duration) time.Duration {
	if a > b {
		return a
	} else {
		return b
	}
}

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
// Each raft server should send an object of this type to its applyCh
// This struct is sent through applyCh for background services to change cfg.logs and cfg.applyErr

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type Status int

const (
	LEADER    Status = 0
	CANDIDATE Status = 1
	FOLLOWER  Status = 2
)

type ElectionResult struct {
	newStatus           Status
	highestTermObserved int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// instance variables for all states
	currentTerm        int
	state              Status
	log                map[int]LogEntry
	applyCh            chan ApplyMsg
	commitIndex        int // last commited index
	lastActiveTime     time.Time
	appendEntryArgsCh  chan *AppendEntriesArgs
	appendEntryReplyCh chan *AppendEntriesReply
	requestVoteArgsCh  chan *RequestVoteArgs
	requestVoteReplyCh chan *RequestVoteReply
	stopServerCh       chan int

	// instance variables for leader
	nextIndices []int // should start appending logs to followers from this index (including)

	// instance variables for voting
	votedFor int // use -1 when this server has not yet voted for anyone
}

func (rf *Raft) resetTimer() {
	rf.lastActiveTime = time.Now()
	// randDuration:=(rand.Intn(ELECTION_TIMEOUT)+ELECTION_TIMEOUT)*time.Millisecond
	// if rf.electionTimeOutTimer==nil{
	// 	rf.electionTimeOutTimer=time.Timer(randDuration)
	// }
	// if !rf.electionTimeOutTimer.Stop(){
	// 	<-rf.electionTimeOutTimer.C
	// }
	// rf.electionTimeOutTimer.Reset(randDuration)
}

func (rf *Raft) resetTimerLock() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetTimer()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int = rf.currentTerm
	var isleader bool = rf.state == LEADER
	// Your code here.
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistLock() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()
}
func (rf *Raft) persist() { //The caller should acquire lock first
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersistLock(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.readPersist(data)
}

func (rf *Raft) readPersist(data []byte) { // The caller should acquire lock first
	// Your code here.
	// Example:
	rf.persister.SaveRaftState(data)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.commitIndex)

}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	CandidateId  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	rf.requestVoteArgsCh <- args
	*reply = *<-rf.requestVoteReplyCh

}

func (rf *Raft) requestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Term[%d] -- Peer[%d] -- State[%d] requestVote(): Request from [%d]\n", rf.currentTerm, rf.me, rf.state, args.CandidateId)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state == LEADER || rf.state == CANDIDATE {
			DPrintf("Term[%d] -- Peer[%d] -- State[%d] requestVote(): Candidate[%d], term[%d] is newer than me, step down\n", rf.currentTerm, rf.me, rf.state, args.CandidateId, args.Term)
			rf.state = FOLLOWER
			rf.nextIndices = []int{}
			rf.votedFor = -1
			rf.resetTimer()
			rf.persist()
		}
	}

	if args.Term == rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.compareLogCompleteness(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		DPrintf("Term[%d] -- Peer[%d] -- State[%d] requestVote(): Vote granted for [%d]\n", rf.currentTerm, rf.me, rf.state, args.CandidateId)

	} else {
		reply.VoteGranted = false
		DPrintf("Term[%d] -- Peer[%d] -- State[%d] requestVote(): Vote declined for [%d]\n", rf.currentTerm, rf.me, rf.state, args.CandidateId)
	}

	reply.Term = rf.currentTerm
	rf.persist()

}

func (rf *Raft) compareLogCompletenessLock(lastLogIndexC, lastLogTermC int) bool {
	return rf.compareLogCompleteness(lastLogIndexC, lastLogTermC)
}

func (rf *Raft) compareLogCompleteness(lastLogIndexC, lastLogTermC int) bool {
	if len(rf.log) == 0 {
		return true
	} else {
		lastLogIndexV := len(rf.log) - 1
		lastLogTermV := rf.log[lastLogIndexV].Term
		if lastLogTermV > lastLogTermC || (lastLogTermV == lastLogTermC && lastLogIndexV > lastLogIndexC) {
			return false
		} else {
			return true
		}
	}
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
// returns true if labrpc says the RPC was delivered.
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	CommitIndex  int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.appendEntryArgsCh <- args
	*reply = *<-rf.appendEntryReplyCh
}

func (rf *Raft) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Term[%d] -- Peer[%d] -- State[%d] AppendEntries(): command from leader[%d], term[%d]\n", rf.currentTerm, rf.me, rf.state, args.LeaderId, args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
		if rf.state == CANDIDATE || rf.state == LEADER {
			DPrintf("Term[%d] -- Peer[%d] -- State[%d] AppendEntries(): leader[%d], term[%d] is newer than me, step down as follower\n", rf.currentTerm, rf.me, rf.state, args.LeaderId, args.Term)

			rf.state = FOLLOWER
			rf.nextIndices = []int{}
			rf.votedFor = -1
			rf.resetTimer()
			rf.persist()

		}

		myPrevLog, ok := rf.log[args.PrevLogIndex]
		myPrevLogTerm := myPrevLog.Term
		if !ok || myPrevLogTerm != args.PrevLogTerm {
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}

		// delete from first non-agreeing entry
		// Append entries
		nonAgreeingDeleted := false
		for i := 0; i < len(args.Entries); i++ {
			if rf.log[args.PrevLogIndex+i+1] != args.Entries[i] {
				if nonAgreeingDeleted == false {
					rfLogLen := len(rf.log)
					for j := args.PrevLogIndex + i + 1; j < rfLogLen; j++ {
						delete(rf.log, j)
					}
					nonAgreeingDeleted = true
				}
				rf.log[args.PrevLogIndex+i+1] = args.Entries[i]

			}
		}
		// advance state machine
		commitIndex := rf.commitIndex
		newCommitIndex := args.CommitIndex
		logCopy := make(map[int]LogEntry)
		for k, v := range rf.log {
			logCopy[k] = v
		}
		go func() {
			for i := commitIndex + 1; i <= newCommitIndex; i++ {
				msg := ApplyMsg{i, logCopy[i].Command, false, []byte{}}
				rf.applyCh <- msg
			}
		}()

		if rf.commitIndex < args.CommitIndex {
			rf.commitIndex = args.CommitIndex
		}

		rf.persist()
		return
	}
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
// TODO change command to interface{}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	index = len(rf.log)
	term = rf.currentTerm
	isLeader = rf.state == LEADER

	if isLeader {
		DPrintf("Term[%d] -- Peer[%d] -- State[%d] Start(): index [%d] - [%d]\n", term, rf.me, rf.state, index, command)
		rf.log[len(rf.log)] = LogEntry{term, len(rf.log) + 1, command}
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
	// Your code here, if desired.
	rf.stopServerCh <- 0
	DPrintf("Term[%d] -- Peer[%d] -- State[%d] ---------------- KILL ------------------\n", rf.currentTerm, rf.me, rf.state)
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

	// Your initialization code here.
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.commitIndex = 0
	rf.applyCh = applyCh
	rf.log = map[int]LogEntry{0: LogEntry{0, 0, 0}}
	rf.appendEntryArgsCh = make(chan *AppendEntriesArgs)
	rf.appendEntryReplyCh = make(chan *AppendEntriesReply)
	rf.requestVoteArgsCh = make(chan *RequestVoteArgs)
	rf.requestVoteReplyCh = make(chan *RequestVoteReply)
	rf.stopServerCh = make(chan int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.nextIndices = []int{}
	rf.votedFor = -1
	rf.resetTimerLock()

	DPrintf("Term[%d] -- Peer[%d] -- State[%d] ---------------- START ------------------\n", rf.currentTerm, rf.me, rf.state)

	// start raft server
	go func() {
		for {
			rf.mu.Lock()
			switch rf.state {
			case FOLLOWER:
				rf.mu.Unlock()
				loop := true
				t := time.Duration(randRange(ELECTION_TIMEOUT_LOWER_BOUND, ELECTION_TIMEOUT_UPPER_BOUND)) * time.Millisecond
				DPrintf("Term[%d] -- Peer[%d] -- State[%d] mainloop(): timeout after %s.\n", rf.currentTerm, rf.me, rf.state,
					fmt.Sprint(t))

				timer := time.After(t)
				for loop {
					select {
					case args := <-rf.appendEntryArgsCh:

						rf.resetTimerLock()
						reply := AppendEntriesReply{}
						rf.appendEntries(args, &reply)
						rf.appendEntryReplyCh <- &reply
					case args := <-rf.requestVoteArgsCh:
						rf.resetTimerLock()
						reply := RequestVoteReply{}
						rf.requestVote(args, &reply)
						rf.requestVoteReplyCh <- &reply

					case <-timer:
						rf.mu.Lock()
						timePassed := time.Now().Sub(rf.lastActiveTime)
						if timePassed < time.Duration(ELECTION_TIMEOUT_UPPER_BOUND)*time.Millisecond {
							t := time.Duration(ELECTION_TIMEOUT_UPPER_BOUND)*time.Millisecond - timePassed
							DPrintf("Term[%d] -- Peer[%d] -- State[%d] mainloop(): reset timeout to be after %s.\n",
								rf.currentTerm, rf.me, rf.state, fmt.Sprint(t))
							timer = time.After(t)
						} else {
							DPrintf("Term[%d] -- Peer[%d] -- State[%d] mainloop(): changed to Candidate due to timeout.\n", rf.currentTerm, rf.me, rf.state)
							loop = false // become candidate
							rf.state = CANDIDATE
							rf.currentTerm++
							rf.nextIndices = []int{}
							rf.votedFor = -1
							rf.resetTimer()
							rf.persist()
						}
						rf.mu.Unlock()

					case <-rf.stopServerCh:
						return
					}
					rf.mu.Lock()
					if rf.state != FOLLOWER {
						rf.mu.Unlock()
						loop = false
					} else {
						rf.mu.Unlock()
					}
				}

			case LEADER:
				rf.mu.Unlock()
				type AppendEntryWorkerResult struct {
					newTerm int
					server  int
					success bool
				}
				appendEntryWorkerCh := make(chan AppendEntryWorkerResult)
				appendEntryWorkerRoutine := func() {

					for {
						rf.mu.Lock()
						if rf.state != LEADER {
							return
						}
						for idx := range rf.peers {
							if idx != rf.me && rf.nextIndices[idx] >= len(rf.log) {
								go func(server int) {
									rf.mu.Lock()
									nextIndex := rf.nextIndices[server]
									rf.mu.Unlock()
									for {
										rf.mu.Lock()
										// removed because heartbeat is needed
										// if rf.nextIndices[server] >= len(rf.log) {
										// 	rf.persist()
										// 	rf.mu.Unlock()
										// 	return
										// }
										if rf.state != LEADER {
											rf.persist()
											rf.mu.Unlock()
											return
										}
										if nextIndex <= 0 {
											log.Fatal("nextIndex cannot be <= 0")
										}
										entries := []LogEntry{}
										for ind := nextIndex; ind < len(rf.log); ind++ {
											entries = append(entries, rf.log[ind])
										}
										prevIndex := nextIndex - 1
										args := AppendEntriesArgs{rf.currentTerm, rf.me, prevIndex,
											rf.log[prevIndex].Term, entries, rf.commitIndex}
										reply := AppendEntriesReply{-1, false}
										rf.mu.Unlock()
										ok := rf.sendAppendEntries(server, &args, &reply)
										rf.mu.Lock()
										if ok {
											if reply.Term > rf.currentTerm {
												awr := AppendEntryWorkerResult{reply.Term, server, reply.Success}
												rf.persist()
												rf.mu.Unlock()
												appendEntryWorkerCh <- awr
												return
											} else if !reply.Success {
												nextIndex--
												rf.mu.Unlock()
											} else if reply.Success {
												rf.nextIndices[server] = len(rf.log)
												rf.persist()
												rf.mu.Unlock()
												return
											}
										}
									}
								}(idx)
							}
						}
						rf.mu.Unlock()
						time.Sleep(time.Duration(LEADER_SLEEP_INTERVAL) * time.Millisecond)
					}

				}
				// TODO: add lock for append entries commiting routine
				leaderCommitRoutine := func() {
					for {
						rf.mu.Lock()
						if rf.state != LEADER {
							return
						}
						newCommitIndex := rf.commitIndex
						for newCommitIndex < len(rf.log) {
							numAppended := 0
							for idx := range rf.peers {
								if rf.nextIndices[idx] > newCommitIndex+1 {
									numAppended++
								}
								if numAppended >= (len(rf.peers)+1)/2 {
									break
								}
							}
							if numAppended >= (len(rf.peers)+1)/2 {
								newCommitIndex++
							}
						}
						if newCommitIndex > rf.commitIndex {
							// advance state machine

							DPrintf("Term[%d] -- Peer[%d] -- State[%d] leaderCommitRoutine(): commiting entries from %d - %d.\n", rf.currentTerm, rf.me, rf.state, rf.commitIndex+1, newCommitIndex)
							logCopy := make(map[int]LogEntry)
							for k, v := range rf.log {
								logCopy[k] = v
							}
							commitIndex := rf.commitIndex
							go func() {
								for i := commitIndex + 1; i <= newCommitIndex; i++ {
									msg := ApplyMsg{i, logCopy[i].Command, false, []byte{}}
									rf.applyCh <- msg
								}
							}()

							rf.commitIndex = newCommitIndex
							rf.persist()
						}
						rf.mu.Unlock()
						time.Sleep(time.Duration(LEADER_SLEEP_INTERVAL) * time.Millisecond)

					}

				}

				go appendEntryWorkerRoutine()
				go leaderCommitRoutine()
				loop := true

				for loop {
					select {
					case args := <-rf.appendEntryArgsCh:
						rf.resetTimerLock()
						reply := AppendEntriesReply{}
						rf.appendEntries(args, &reply)
						rf.appendEntryReplyCh <- &reply
					case args := <-rf.requestVoteArgsCh:
						rf.resetTimerLock()
						reply := RequestVoteReply{}
						rf.requestVote(args, &reply)
						rf.requestVoteReplyCh <- &reply
					case appendMsg := <-appendEntryWorkerCh:
						rf.mu.Lock()
						loop = false
						rf.state = FOLLOWER
						rf.currentTerm = appendMsg.newTerm
						rf.nextIndices = []int{}
						rf.votedFor = -1
						rf.resetTimer()
						rf.persist()
						DPrintf("Term[%d] -- Peer[%d] -- State[%d] mainloop(): step down when appending entry to others.\n", rf.currentTerm, rf.me, rf.state)
						rf.mu.Unlock()
					case <-rf.stopServerCh:
						return

					}
				}

			case CANDIDATE:
				rf.mu.Unlock()
				electionResultCh := make(chan ElectionResult)

				requestVoteRoutine := func() {
					rf.mu.Lock()
					votedServers := []int{}
					// Vote self
					votedServers = append(votedServers, rf.me)
					rf.votedFor = rf.me
					rf.persist()

					// request for votes

					requestVoteReplyChan := make(chan *RequestVoteReply)
					args := RequestVoteArgs{rf.me, rf.currentTerm, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}
					currentTerm := rf.currentTerm
					for idx := range rf.peers { // send vote request to everyone
						if idx != rf.me {
							go func(server int) {
								replyPtr := &RequestVoteReply{-1, false}
								ok := rf.sendRequestVote(server, &args, replyPtr)
								if !ok {
									replyPtr = nil
								}
								select {
								case requestVoteReplyChan <- replyPtr:
									if replyPtr != nil && replyPtr.VoteGranted {
										DPrintf("Term[%d] -- Peer[%d] -- State[%d] requestVoteRoutine(): vote from [%d] - %v\n",
											currentTerm, me, CANDIDATE, server, replyPtr.VoteGranted)
									}
								case <-time.After(time.Duration(ELECTION_TIMEOUT_UPPER_BOUND) * time.Millisecond):
								}
							}(idx)
						}
					}

					DPrintf("Term[%d] -- Peer[%d] -- State[%d] requestVoteRoutine(): waiting votes\n", rf.currentTerm, rf.me, rf.state)
					total := len(rf.peers)
					receivedVotes := 1 // 1 vote for self
					highestTermObserved := rf.currentTerm
					rf.mu.Unlock()
					for i := 0; i < total-1; i++ {
						reply := <-requestVoteReplyChan
						if reply != nil {
							if reply.Term > highestTermObserved {
								highestTermObserved = reply.Term
							}
							if reply.VoteGranted {
								receivedVotes++
							}
							if receivedVotes >= (total+1)/2 {
								break
							}
						}
					}
					rf.mu.Lock()
					if highestTermObserved > rf.currentTerm {
						er := ElectionResult{FOLLOWER, highestTermObserved}
						DPrintf("Term[%d] -- Peer[%d] -- State[%d] requestVoteRoutine(): election finished, highestTermObserved = %d, advice stepping down", rf.currentTerm, rf.me, rf.state, highestTermObserved)
						rf.mu.Unlock()
						electionResultCh <- er

						return
					} else {
						DPrintf("Term[%d] -- Peer[%d] -- State[%d] requestVoteRoutine(): received votes [%d/%d].\n",
							rf.currentTerm, rf.me, rf.state, receivedVotes, total)
						if receivedVotes >= (total+1)/2 {
							er := ElectionResult{LEADER, highestTermObserved}
							DPrintf("Term[%d] -- Peer[%d] -- State[%d] requestVoteRoutine(): advice changing to Leader.\n", rf.currentTerm, rf.me, rf.state)
							rf.mu.Unlock()
							electionResultCh <- er

						} else {
							er := ElectionResult{CANDIDATE, highestTermObserved}
							DPrintf("Term[%d] -- Peer[%d] -- State[%d] requestVoteRoutine(): advice staying as candidate.\n", rf.currentTerm, rf.me, rf.state)
							rf.mu.Unlock()
							electionResultCh <- er
						}
					}
				}

				go requestVoteRoutine()
				t := time.Duration(randRange(ELECTION_TIMEOUT_LOWER_BOUND, ELECTION_TIMEOUT_UPPER_BOUND)) * time.Millisecond
				DPrintf("Term[%d] -- Peer[%d] -- State[%d] mainloop(): timeout after %s.\n",
					rf.currentTerm, rf.me, rf.state, fmt.Sprint(t))
				timer := time.After(t * time.Millisecond)
				loop := true
				for loop {
					select {
					case args := <-rf.appendEntryArgsCh:
						rf.resetTimerLock()
						reply := AppendEntriesReply{}
						rf.appendEntries(args, &reply)
						rf.appendEntryReplyCh <- &reply
					case args := <-rf.requestVoteArgsCh:
						rf.resetTimerLock()
						reply := RequestVoteReply{}
						rf.requestVote(args, &reply)
						rf.requestVoteReplyCh <- &reply
					case er := <-electionResultCh:
						rf.mu.Lock()
						switch er.newStatus {
						case FOLLOWER:
							DPrintf("Term[%d] -- Peer[%d] -- State[%d] mainloop(): candidate changed to follower.\n", rf.currentTerm, rf.me, rf.state)
							loop = false
							rf.state = FOLLOWER
							rf.currentTerm = er.highestTermObserved
							rf.nextIndices = []int{}
							rf.votedFor = -1
							rf.resetTimer()
							rf.persist()
						case CANDIDATE:
							DPrintf("Term[%d] -- Peer[%d] -- State[%d] mainloop(): stay as candidate.\n", rf.currentTerm, rf.me, rf.state)
							loop = false // wait for appendEntry and requestVote and timer if not elected
							rf.state = CANDIDATE
							rf.currentTerm++
							rf.nextIndices = []int{}
							rf.votedFor = -1
							rf.resetTimer()
							rf.persist()
						case LEADER:
							loop = false
							DPrintf("Term[%d] -- Peer[%d] -- State[%d] mainloop(): candidate changed to leader.\n", rf.currentTerm, rf.me, rf.state)
							rf.state = LEADER
							rf.nextIndices = make([]int, len(rf.peers))
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndices[i] = len(rf.log)
							}
							rf.votedFor = -1
							rf.resetTimer()
							rf.persist()
						default:
							log.Fatal("Unknown state " + strconv.Itoa(int(rf.state)))
						}
						rf.mu.Unlock()
					case <-timer:
						rf.mu.Lock()
						timePassed := time.Now().Sub(rf.lastActiveTime)

						if timePassed < time.Duration(ELECTION_TIMEOUT_UPPER_BOUND)*time.Millisecond {
							t := time.Duration(ELECTION_TIMEOUT_UPPER_BOUND)*time.Millisecond - timePassed
							DPrintf("Term[%d] -- Peer[%d] -- State[%d] mainloop(): reset timeout to be after %s.\n",
								rf.currentTerm, rf.me, rf.state, fmt.Sprint(t))
							timer = time.After(t)
						} else {
							DPrintf("Term[%d] -- Peer[%d] -- State[%d] mainloop(): election timeout, start new term.\n", rf.currentTerm, rf.me, rf.state)
							loop = false // candidate for next term
							rf.state = CANDIDATE
							rf.currentTerm++
							rf.nextIndices = []int{}
							rf.votedFor = -1
							rf.resetTimer()
							rf.persist()
						}
						rf.mu.Unlock()
					case <-rf.stopServerCh:
						return
					}
					rf.mu.Lock()
					if rf.state != CANDIDATE {
						rf.mu.Unlock()
						loop = false
					} else {
						rf.mu.Unlock()
					}
				}

			default:
				rf.mu.Unlock()
				log.Fatal("Unknown state " + strconv.Itoa(int(rf.state)))
			}
		}
	}()

	return rf
}
