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

// import "bytes"
// import "encoding/gob"

const {
	Stopped = "stopped"
	Follower = "follower"
	Candidate = "candidates"
	Leader = "leader" 
}
const ElectionTimeoutThresholdPercent = 0.8
type ev struct {
	target interface{}
	returnValue interface{}
	c chan error
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

type LogEntries struct {
	command interface{}
	term int
}
//log entries 外面再包一层
type Log struct {
	//ApplyFunc func(*LogEntries) (interface{}, error)
	entries []*LogEntries
	commitIndex int
	mutex sync.Mutex
	startIndex int
	initialized bool
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	leader    int                 // this peer's leader
	state     string
	currentTerm int
	votedFor int
	routineGroup sync.WaitGroup
	//log  *Log
	//commitIndex int
	lastApplied int
    //nextIndex   []int              //对于每一个服务器，
    //需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一） 
    //matchIndex  []int              //对于每一个服务器，已经复制给他的日志的最高索引值      
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	c  chan *ev
}

func (rf *Raft) State() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}
func (rf *Raft) setState(s string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = s
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if state == "leader" {
		term = rf.currentTerm
		isleader = true
	} 
	else {
		term = rf.currentTerm
		isleader = false
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int 
	CandidatedId int
	LastLogIndex int
	LastLogTerm int
	Term int
	VoteGrantedtrue bool

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	VoteForCandidate bool
}

func afterBetween(min time.Duration, max time.Duration) <-chan time.Time {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	d, delta := min, (max - min)
	if delta > 0 {
		d += time.Duration(rand.Int63n(int64(delta)))
	}
	return time.After(d)
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	
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

	// Your code here (2B).


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
func (rf *Raft) loop() {
	defer DPrintf("server.loop.end\n")
	state := rf.State()

	for state != Stopped {
		DPrintf("raft.loop.run %s\n", state)
		switch state {
		case Follower:
			rf.followerLoop()
		case Candidate:
			rf.candidateLoop()
		case Leader:
			rf.leaderLoop()
		}
		state = rf.State()
	}

}
func (rf *Raft) followerLoop() {
	since := time.Now()
	electionTimeout := RaftElectionTimeout
	timeoutchan := afterBetween(electionTimeout, electionTimeout * 2)
	for rf.State() == Follower {
		var err error
		update := false
		select {
		case e := <- rf.c:
			switch req := e.target.(type) {
			case *RequestVoteRequest:
				elapsedTime := time.Now().Sub(since)
				if elapsedTime > time.Duration(float64(RaftElectionTimeout)*ElectionTimeoutThresholdPercent) {
					rf.DispatchEvent(newEvent(ElectionTimeoutThresholdEventType, elapsedTime, nil) )
				}
				e.returnValue, update = rf.processAppendEntriesRequest(req)
			case *RequestVoteRequest:
				e.returnValue, update = rf.processRequestVoteRequest(req)
			default:
				err = NotLeaderError
			}
			//call back to event
			//this step will block
			e.c <- err
		case <- timeoutChan:
			//todo: only allow synced follower to promote to candidate
			s.setState(Candidate)
		}
		if update {
			since = time.Now()
			timeoutChan = 
		}

	}

}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.c = make(chan *ev, 256)

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.setState(Follower)
	DPrintf("start one raft server\n")
	rf.routineGroup.Add(1)
	go func() {
		defer rf.routineGroup.Done()
		rf.loop()
	}

	return rf
}
