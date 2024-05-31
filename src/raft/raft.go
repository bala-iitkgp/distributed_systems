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

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//"fmt"

	"6.5840/labgob"
	"6.5840/labrpc"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Logs struct{
	Command interface{}
	Term int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor int
	log []Logs
	commitLength int

	currentRole string
	currentLeader int
	votesReceived map[int]bool

	sentLength []int
	ackedLength []int

	applyCh chan ApplyMsg

	// declare a bool variable receivedMsg -> set to true when you receive a msg, check this
	// before triggering an election and then set this to false
	receivedMsg bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var isLeader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	if (rf.currentRole == "leader") {
		isLeader = true
	} else {
		isLeader = false
	}
	
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.commitLength)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Logs
	var commitLength int
	if (d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&log) != nil ||
	   d.Decode(&commitLength) != nil ) {
		////fmt.Println("Decode Error!!")	
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.commitLength = commitLength
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	CandidateId int
	CandidateTerm int
	//Below details are for 3B
	CLogLength int
	CLogTerm int
	
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	VoterId int
	VCurrentTerm int
	VoterResponse bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	cId := args.CandidateId
	cTerm := args.CandidateTerm
	cLogLength := args.CLogLength
	cLogTerm := args.CLogTerm

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < cTerm {
		rf.currentTerm = cTerm
		rf.currentRole = "follower"
		// remove any votedFor in previous terms
		rf.votedFor = -1
		rf.persist()
	}

	lastTerm := 0
	if(len(rf.log) > 0) {
		lastTerm = rf.log[len(rf.log)-1].Term
	}

	logOk := (cLogTerm > lastTerm ) || (cLogTerm == lastTerm && cLogLength >= len(rf.log))
	if cTerm == rf.currentTerm && logOk && (rf.votedFor == -1 || rf.votedFor == cId) {
		rf.votedFor = cId
		rf.persist()
		reply.VoterResponse = true
	} else {
		reply.VoterResponse = false
	}
	reply.VoterId = rf.me
	reply.VCurrentTerm = rf.currentTerm

	return

}

type AppendEntriesArgs struct {
	LeaderId int
	LeaderTerm int
	PrefixLength int
	PrefixTerm int
	LeaderCommit int
	Suffix []Logs
}

type AppendEntriesReply struct {
	FollowerId int
	FollowerTerm int
	AckedLength int
	IsSuccess bool
	XTerm int
	XIndex int
	XLen int
}

func (rf *Raft) addLogs(prefixLen int, leaderCommit int, suffix []Logs) {

	rf.log = rf.log[:prefixLen]
	for i:= 0 ; i< len(suffix) ; i++ {
		rf.log = append(rf.log, suffix[i])
	}
	rf.persist()

	if leaderCommit > rf.commitLength {
		for j:= rf.commitLength; j < leaderCommit; j++ {
			//Deliver log[j]
			msg := ApplyMsg{}
			msg.CommandValid = true
			msg.CommandIndex = j
			msg.Command = rf.log[j].Command
			rf.applyCh <- msg
			rf.commitLength = rf.commitLength + 1
			rf.persist()
		}
		rf.commitLength = leaderCommit
	}
	rf.persist()
}

func (rf *Raft) findXIndex(prefixLen int) int {
	var xIndex = prefixLen - 1
	for ; xIndex > 0 && rf.log[xIndex -1].Term == rf.log[prefixLen -1].Term ; xIndex-- {

	}
	return xIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	//fmt.Println("At ", rf.me, " appendEntries ", " prefixLen: ", args.PrefixLength, " suffix: ", len(args.Suffix))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if args.LeaderTerm > rf.currentTerm {
		rf.currentTerm = args.LeaderTerm
		rf.votedFor = -1
		rf.receivedMsg = true
		rf.persist()
	} 
	if args.LeaderTerm == rf.currentTerm {
		rf.currentRole = "follower"
		rf.currentLeader = args.LeaderId
		rf.receivedMsg = true
	}

	logOk := ( len(rf.log)>= args.PrefixLength ) && 
			(args.PrefixLength ==0 || rf.log[args.PrefixLength-1].Term == args.PrefixTerm)
	//fmt.Println("logOk: ", logOk, " len of rf.log ", len(rf.log), " prefixLength: ",args.PrefixLength)
	if (args.LeaderTerm == rf.currentTerm) && logOk {
		reply.FollowerId = rf.me
		reply.FollowerTerm = rf.currentTerm
		rf.addLogs(args.PrefixLength, args.LeaderCommit, args.Suffix)
		reply.AckedLength = args.PrefixLength + len(args.Suffix)
		reply.IsSuccess = true
	} else {
		reply.FollowerId = rf.me
		reply.FollowerTerm = rf.currentTerm
		reply.AckedLength = 0
		reply.IsSuccess = false
		if(len(rf.log)>=args.PrefixLength){
			reply.XTerm = rf.log[args.PrefixLength-1].Term
			reply.XIndex = rf.findXIndex(args.PrefixLength)
		} else{
			reply.XTerm = -1
		}
		reply.XLen = len(rf.log)
	}

	return
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	term, isLeader = rf.GetState()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if isLeader {
		index = len(rf.log)
		nextLog := Logs{}
		nextLog.Command = command
		nextLog.Term = rf.currentTerm
		rf.log = append(rf.log, nextLog)
	}
	

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) generateSuffix(prefixLen int) []Logs {
	var suffix []Logs

	var id int

	for id = prefixLen ;id < len(rf.log); id++ {
		suffix = append(suffix, rf.log[id])
	}

	return suffix
}

func (rf *Raft) startElection() {

	//fmt.Println("An election is being started")
	args := RequestVoteArgs{}
	args.CandidateId = rf.me
	args.CandidateTerm = rf.currentTerm

	//to be added in 3B
	args.CLogLength = len(rf.log)
	if (len(rf.log) != 0) {
		args.CLogTerm = rf.log[len(rf.log)-1].Term
	} else{
		args.CLogTerm = 0
	}

	for i, _ := range rf.peers {
		
		if (i != rf.me){
			go func(x int){
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(x, &args, &reply)
				
				//Rethink: if this makes sense - instead we should keep trying to send this and keep a timer of electiontimeout
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				

				if rf.currentRole == "candidate" && reply.VCurrentTerm == rf.currentTerm && reply.VoterResponse {
					rf.votesReceived[reply.VoterId] = true

					if (len(rf.votesReceived) >= (len(rf.peers)+2)/2){
						//fmt.Println("Leader Alert!")
						//fmt.Println("current leader: ",rf.me, " at term: ", rf.currentTerm)
						rf.currentRole = "leader"
						rf.currentLeader = rf.me
						
						for j, _ := range rf.peers{
							if (j != rf.me) {
								rf.sentLength[j] = len(rf.log)
								rf.ackedLength[j] = 1
								//Rethink: If this args can be taken outside of this go func
								args := AppendEntriesArgs{}
								args.LeaderId = rf.me
								args.LeaderTerm = rf.currentTerm
								args.PrefixLength = len(rf.log)
								args.PrefixTerm = 0
								if args.PrefixLength > 0 {
									args.PrefixTerm = rf.log[args.PrefixLength - 1].Term
								}
								args.Suffix = rf.generateSuffix(args.PrefixLength)
								go func(y int) {
									reply := AppendEntriesReply{}
									rf.sendAppendEntries(y, &args, &reply)
								}(j)
							}
						}

						return
					}
				} else if reply.VCurrentTerm > rf.currentTerm {
					rf.currentTerm = reply.VCurrentTerm
					rf.currentRole = "follower"
					rf.votedFor = -1
					rf.persist()
					return
				} else if rf.currentRole == "leader" {
					return
				} else 	{
					return
				}
			} (i)
		}
	}


}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		//TODO: check if this time makes sense
		ms := 190 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		currentRole := rf.currentRole

		if currentRole != "leader" {
			receivedMsg := rf.receivedMsg
			if !receivedMsg {
				rf.currentTerm = rf.currentTerm +1
				rf.currentRole = "candidate"
				rf.votedFor = rf.me
				rf.persist()
				//clear the set votesReceived
				for k := range rf.votesReceived {
					delete(rf.votesReceived, k)
				}
				rf.votesReceived[rf.me] = true
				rf.startElection()
			} else {
				rf.receivedMsg = false
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) commitLogs() {

	var acks int

	if rf.currentRole == "leader" {
		
		for ; rf.commitLength < len(rf.log); {
			acks = 1
			for i, _ := range rf.peers {
				if i!= rf.me {
					if rf.ackedLength[i] > rf.commitLength {
						acks = acks + 1
					}
				}
			}

			if (acks >= (len(rf.peers)+2)/2 ) {
				msg := ApplyMsg{}
				msg.CommandValid = true
				msg.CommandIndex = rf.commitLength
				msg.Command = rf.log[rf.commitLength].Command
				rf.applyCh <- msg
				rf.commitLength = rf.commitLength + 1
				rf.persist()
			} else {
				break;
			}
		}
	}
}

func (rf* Raft) findLastIndexOfXTerm(xTerm int) int {
	lastIndex := len(rf.log)

	for ;lastIndex > 0 && rf.log[lastIndex-1].Term > xTerm; lastIndex-- {}

	if (lastIndex <0) {
		return 0
	} else if(rf.log[lastIndex-1].Term == xTerm) {
		return lastIndex
	} else {
		return -1;
	}
}

func (rf* Raft) sendLogsToFollower(x int, leaderId int, leaderTerm int, leaderCommit int){
	rf.mu.Lock()
	//fmt.Println("Sending logs to follower: ", x, " by leader: ", leaderId)
	args := AppendEntriesArgs{}
	args.LeaderId = leaderId
	args.LeaderTerm = leaderTerm
	args.LeaderCommit = leaderCommit
	//fmt.Println("For ",x , " sentLength: ", rf.sentLength[x] )
	args.PrefixLength = rf.sentLength[x]
	args.PrefixTerm = 0
	if args.PrefixLength > 0 {
		args.PrefixTerm = rf.log[args.PrefixLength - 1].Term
	}
	args.Suffix = rf.generateSuffix(args.PrefixLength)
	reply := AppendEntriesReply{}
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(x, &args, &reply)
	if !ok {
		return
	}
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.FollowerTerm == rf.currentTerm && rf.currentRole == "leader"{
		if reply.IsSuccess == true {
			rf.sentLength[x] = reply.AckedLength
			rf.ackedLength[x] = reply.AckedLength
			rf.commitLogs()
		} else if rf.sentLength[x] > 0 {
			if reply.XTerm == -1 {
				rf.sentLength[x] = reply.XLen
			} else {
				//rf.sentLength[x] = rf.sentLength[x] -1
				lastIndex := rf.findLastIndexOfXTerm(reply.XTerm)
				if lastIndex == -1 {
					rf.sentLength[x] = reply.XIndex
				} else {
					rf.sentLength[x] = lastIndex
				}
			}
			args.PrefixLength = rf.sentLength[x]
			args.Suffix = rf.generateSuffix(args.PrefixLength)
			_ = rf.sendAppendEntries(x, &args, &reply)
		}
	} else if reply.FollowerTerm > rf.currentTerm && rf.currentRole == "leader" {
		rf.currentTerm = reply.FollowerTerm
		rf.currentRole = "follower"
		rf.votedFor = -1
		rf.persist()
	}
}

func (rf* Raft) sendLeaderLogs() {

	for rf.killed() == false {
		_, leader := rf.GetState()
		if leader {
			rf.mu.Lock()
			leaderId := rf.me
			leaderTerm := rf.currentTerm
			leaderCommit := rf.commitLength
			rf.mu.Unlock()
			for i, _ := range rf.peers {
				if (i!= rf.me) {
					go func(x int) {
						rf.sendLogsToFollower(x, leaderId, leaderTerm, leaderCommit)		
					} (i)
				}
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	
	rf.currentRole = "follower"
	rf.currentLeader = -1
	rf.votesReceived = make(map[int]bool)

	rf.currentTerm = 0
	rf.receivedMsg = false
	
	rf.votedFor = -1

	rf.applyCh = applyCh

	rf.commitLength = 1
	initLog := Logs{}
	initLog.Term = 0
	rf.log = []Logs{initLog}

	//initializing sentLength & ackLength
	for i:=0; i< len(rf.peers) ; i++ {
		rf.sentLength = append(rf.sentLength, 0)
		rf.ackedLength = append(rf.ackedLength, 0)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start leader logs goroutine to notify followers
	go rf.sendLeaderLogs()


	return rf
}
