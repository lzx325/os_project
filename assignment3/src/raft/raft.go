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
const ELECTION_TIMEOUT int=250 // add randomness
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

type LogEntry struct{
	term int
	index int
	command int
}

type Status int
const(
	LEADER Status = 0
	CANDIDATE Status = 1
	FOLLOWER Status = 2
)


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
	currentTerm int
	state       Status
	log			map[int]*LogEntry
	applyCh chan ApplyMsg
	commitIndex int
	electionTimeOutTimer *time.Timer
	appendEntryArgsCh chan AppendEntriesArgs
	appendEntryReplyCh chan *AppendEntriesReply
	requestVoteArgsCh chan RequestVoteArgs
	requestVoteReplyCh chan *RequestVoteReply

	stopServerCh chan int

	// instance variables for leader
	nextIndices []int

	// instance variables for voting
	votedFor int // use -1 when this server has not yet voted for anyone
}

func(rf *Raft) resetTimer(){
	randDuration:=(rand.Intn(ELECTION_TIMEOUT)+ELECTION_TIMEOUT)*time.Millisecond
	if rf.electionTimeOutTimer==nil{
		rf.electionTimeOutTimer=time.Timer(randDuration)
	}
	if !rf.electionTimeOutTimer.Stop(){
		<-rf.electionTimeOutTimer.C
	}
	rf.electionTimeOutTimer.Reset(randDuration)
}

func(rf *Raft) resetTimerLock(){
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
	var isleader bool = rf.state==LEADER
	// Your code here.
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistLock(){
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
func (rf *Raft) readPersistLock(data []byte){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.readPersist()
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
	candidateId  int
	term         int
	lastLogIndex int
	lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	term        int
	voteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.requestVoteArgsCh<-args
	*reply=<-rf.requestVoteReplyCh

}

func (rf *Raft) requestVote(args RequestVoteArgs, reply *RequestVoteReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.term > rf.currentTerm {
		rf.currentTerm = args.term
		if rf.state == LEADER || rf.state == CANDIDATE {
			rf.state = FOLLOWER
		}
	} 
	
	if args.term == rf.currentTerm\ 
	&& (rf.votedFor == -1 || rf.votedFor == args.candidateId) \
	&& rf.compareLogCompleteness(args.lastLogIndex,args.lastLogTerm) {
		reply.voteGranted=true
		rf.votedFor=args.candidateId
	} else{
		reply.voteGranted=false
	}
	reply.term=rf.currentTerm
	rf.persist()
	rf.resetTimer()

}

func (rf *Raft) compareLogCompletenessLock(lastLogIndexC,lastLogTermC) bool{
	return rf.compareLogCompleteness(lastLogIndexC,lastLogTermC)
}

func (rf *Raft) compareLogCompleteness(lastLogIndexC,lastLogTermC) bool{
	if len(rf.log)==0{
		return true
	} else{
		lastLogIndexV=len(rf.log)-1
		lastLogTermV=rf.log[lastLogIndexV].term
		if lastLogTermV>lastLogTermC || (lastLogTermV==lastLogTermC && lastLogIndexV>lastLogIndexC){
			return false
		} else{
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC
type AppendEntriesArgs struct{
	term int
	leaderId int
	prevLogIndex int
	prevLogTerm int
	entries []int
	commitIndex int
}

type AppendEntriesReply struct{
	term int
	success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs,reply *AppendEntriesReply){
	rf.appendEntryArgsCh<-args
	*reply=*rf.appendEntryReplyCh
}

func (rf *Raft) appendEntries(args AppendEntriesArgs,reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.UnLock()
	if args.term<rf.currentTerm{
		reply.term=rf.currentTerm
		reply.success=false
		return
	} else{
		if args.term>rf.currentTerm{
			rf.currentTerm=args.term
		}
		if rf.state==CANDIDATE || rf.state == LEADER{
			rf.state=FOLLOWER
		}

		//Reset Election Timeout
		rf.resetTimer(ELECTION_TIMEOUT)

		myPrevLogTerm, ok:= rf.log[args.prevLogIndex]
		if !ok || myPrevLogTerm!=args.prevLogTerm{
			reply.term=rf.currentTerm
			reply.success=false
			rf.persist()
			return
		}

		// delete from first non-agreeing entry
		// Append entries
		nonAgreeingDeleted:=false
		for i:=0;i<len(args.entries);i++{
			if rf.log[args.prevLogIndex+i+1] != args.entries[i] {
				if nonAgreeingDeleted==false{
					rfLogLen:=len(rf.log)
					for j:=args.prevLogIndex+i+1;j<rfLogLen;j++{
						delete(rf.log,j)
					}
					nonAgreeingDeleted=true
				}
				rf.log[args.prevLogIndex+i+1]=args.entries[i]

			}
		}
		// advance state machine
		for i:=rf.commitIndex+1;i<=args.commitIndex;i++{
			msg=ApplyMsg{i,rf.log[i].command,false,[]byte{}}
			rf.applyCh<-msg
		}

		if rf.commitIndex<args.commitIndex{
			rf.commitIndex=args.commitIndex
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

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
	rf.stopServerCh<-0
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
	rf.currentTerm=0
	rf.state=FOLLOWER
	rf.commitIndex=-1
	rf.applyCh=applyCh
	rf.stopServerCh=make(chan int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.nextIndices=make([]int,len(rf.peers))
	rf.votedFor=-1
	
	// start raft server
	go func(){
		rf.resetTimerLock()
		for{
			rf.mu.Lock()
			switch rf.state{
			case FOLLOWER:
				rf.mu.Unlock()
				loop:=true
				for loop{
					select{
					case args:=<-rf.appendEntryArgsCh:
						reply:=AppendEntriesReply{}
						rf.appendEntries(args,&reply)
						rf.appendEntryReplyCh<-&reply
					case args:=<-rf.requestVoteArgsCh:
						reply:=RequestVoteReply{}
						rf.requestVote(args,&reply)
						rf.requestVoteReplyCh<-&reply
					case <-rf.electionTimeOutTimer.C:
						rf.state=CANDIDATE
						loop=false
					case <-rf.stopServerCh:
						return
					}
				}
				
			case LEADER:
				rf.mu.Unlock()
			case CANDIDATE:
			
				rf.currentTerm++
				rf.persist()
				voteResultCh:=make(chan bool)

				requestVoteRoutine:=func(){
					rf.mu.Lock()
					defer rf.mu.Unlock()
					votedServers:=[]int{}
					// Vote self
					votedServers=append(votedServers,rf.me)
					rf.votedFor=rf.me
					rf.persist()

					// reset election timeout
					rf.resetTimer()
					

				}
				rf.mu.Unlock()

				loop:=true
				for loop{
					select{
					case args:=<-rf.appendEntryArgsCh:
						reply:=AppendEntriesReply{}
						rf.appendEntries(args,&reply)
						rf.appendEntryReplyCh<-&reply
					case args:=<-rf.requestVoteArgsCh:
						reply:=RequestVoteReply{}
						rf.requestVote(args,&reply)
						rf.requestVoteReplyCh<-&reply
					}
					rf.mu.Lock()
					if rf.state!=CANDIDATE{
						rf.mu.Unlock()
						loop=false
					} else{
						rf.mu.Unlock()
					}
				}
							
			default:
				rf.mu.Unlock()
				log.Fatal("Unknown state "+strconv.Itoa(rf.state))
			}
		}
	}()


	return rf
}
