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
import "errors"

// import "bytes"
// import "encoding/gob"

const {
	Stopped = "stopped"
	Follower = "follower"
	Candidate = "candidates"
	Leader = "leader" 
}

const ElectionTimeoutThresholdPercent = 0.8
var StopError = errors.New("raft: Has been stopped")

type ev struct {
	target interface{}
	returnValue interface{}
	c chan error
}
// Sends an event to the event loop to be processed. The function will wait
// until the event is actually processed before returning.
// Send 函数将RPC送来的请求放到eventloop的chan里，等到eventloop处理完之后返回
// 类似 go-raft实现中通过http post传过来的请求参数经过send函数交给eventloop处理
func (rf *raft) Running() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return s.state != Stopped
}

func (rf *raft) send(value interface{}) (interface{}, error) {
	if !s.Running() {
		return nil, StopError
	}

	event := &ev{target: value, c: make(chan error, 1)}
	select {
	case s.c <- event:
	case <-s.stopped:
		return nil, StopError
	}
	select {
	case <-s.stopped:
		return nil, StopError
	case err := <-event.c:
		return event.returnValue, err
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
//原始代码中的peer只是labrpc.ClientEnd，不便于进行封装，在这里重新进行封装
type Peer struct {
	raft *Raft
	ConnectClient *labrpc.ClienEnd
	//prevLogIndex int   
	//后面在实现log replication的时候加入
	stopChan chan bool
	heartbeatInterval time.Duration
	lastActivity time.Time
	sync.Mutex
}
func newPeer(raft *raft, peerid int, connectclient *labrpc.ClienEnd, heartbeatInterval time.Duration) *Peer{
	return &Peer{
		raft:            raft,
		ConnectClient:   connectclient,
		heartbeatInterval: heartbeatInterval,
	}
}
//starts the peer heartbeat
func (p *Peer) startHeartbeat() {
	p.stopChan = make(chan bool)
	c := make(chan bool)
	p.setLastActivity(time.Now())
	p.raft.routineGroup.Add(1)
	go func() {
		defer p.raft.routineGroup.Done()
		p.heartbeat(c)
	}()
	<-c
}
func (p *Peer) heartbeat(c chan bool) {
	stopChan := p.stopChan
	c <- true
	ticker := time.Tick(p.heartbeatInterval)
	DPrintf("peer.heartbeat: ", p.ConnectClient.endname, p.heartbeatInterval)
	for {
		select {
		case flush := <-stopChan:
			if flush {
				// before we can safely remove a node
				// we must flush the remove command to the node first
				p.flush()
				debugln("peer.heartbeat.stop.with.flush: ", p.Name)
				return
			} else {
				debugln("peer.heartbeat.stop: ", p.Name)
				return
			}
		case <-ticker:
			start := time.Now()
			p.flush()
			
		}
	}	
}
func (p *Peer) flush() {
	DPrintf("peer.heartbeat.flush: ", p.ConnectClient.endname)
	term := p.raft.currentTerm
	//
	p.sendAppendEntriesRequest(AppendEntriesRequest(term))
}


type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*Peer // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	leader    int                 // this peer's leader
	state     string
	currentTerm int
	votedFor int
	routineGroup sync.WaitGroup  //raft需要去等着他开出去的goroutine工作结束
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
	stopped chan bool
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
func (rf *Raft) Term() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
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
	Term int
	VoteForCandidate bool
}
func newRequestVoteReply(term int, voteForCandidate bool) *RequestVoteReply {
	return &RequestVoteReply {
		Term: term
		VoteForCandidate: voteForCandidate
	}
}

type AppendEntriesRequest struct {
	Term int
	Leader int 
}

type AppendEntriesReply struct {
	Term int
	Success bool
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
// 这里的RequestVote类似go-raft中的http handler，收到请求后发送到chan里后等待处理结束
// outgoing call
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//RPC的调用示例在这里
	ok := rf.peers[server].ConnectClient.Call("Raft.RequestVote", args, reply)
	return ok
} 
// ingoing call
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	ret, _ := rf.send(args)
	reply := ret.(*RequestVoteReply)
}
func (rf *Raft) sendAppendEntriesRequest(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].ConnectClient.Call("Raft.RequestAppendEntries", args, reply)
	return ok
}
func (rf *Raft) AppendEntriesRequest(args *RequestVoteArgs, reply *RequestVoteReply) {
	ret, _ := rf.send(args)
	reply := ret.(*RequestVoteReply)
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

func (rf *Raft) processRequestVoteRequest(req *RequestVoteArgs) (*RequestVoteReply, bool) {
	// If the request is coming from an old term then reject it.
	if req.Term < rf.Term() {
		DPrintf("server.rv.deny.vote: cause stale term")
		return newRequestVoteReply(rf.currentTerm, false), false
		//这两个false的意思是既不给该candidate进行投票，该请求也不能作为一个心跳来维持该follower的在线状态
	}
	// If the term of the request peer is larger than this node, update the term
	// If the term is equal and we've already voted for a different candidate then
	// don't vote for this candidate.
	if req.Term > rf.Term() {
		rf.updateCurrentTerm(req.Term, -1)
	} else if rf.votedFor != -1 && rf.votedFor != req.CandidatedId {
		DPrintf("server.deny.vote: cause duplicate vote: ", req.CandidatedId, 
			" already vote for", rf.votedFor)
		return newRequestVoteReply(rf.currentTerm, false), false
	}
	//在test 2A中暂时不考率log的index问题（和log耦合的部分），raft设计的思路依旧遵照软件工程的高内聚，低耦合
	//此时已经可以给该candidate进行投票了
	DPrintf("server.rv.vote: ", rf.name, "votes for", req.CandidatedId, "at term", req.Term)
	rf.votedFor = req.CandidatedId
	return newRequestVoteReply(s.currentTerm, true), true
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

func (rf *server) updateCurrentTerm(term int, leaderName int) {
	_assert(term > s.currentTerm,
		"upadteCurrentTerm: update is called when term is not larger than currentTerm")

	// Store previous values temporarily.
	prevTerm := rf.currentTerm
	prevLeader := rf.leader

	// set currentTerm = T, convert to follower (§5.1)
	// stop heartbeats before step-down
	if rf.state == Leader {
		for _, peer := range rf.peers {
			peer.stopHeartbeat(false)
		}
	}
	// update the term and clear vote for
	if s.state != Follower {
		s.setState(Follower)
	}

	rf.mu.Lock()
	rf.currentTerm = term
	rf.leader = leaderName
	// -1 means no voteFor
	rf.votedFor = -1
	rf.mu.Unlock()
}

func (rf *Raft) processAppendEntriesRequest (req *AppendEntriesReply) (*AppendEntriesReply, bool) {
	//part 2A do not involve the log, only for heartbeat
	DPrintf("server.ae.process")
	if req.Term < rf.currentTerm {
		DPrintf("server.ae.error: stale term")
		return newAppendEntriesResponse(rf.currentTerm), false
	}
	if req.Term == rf.currentTerm {
		_assert(s.State() != Leader, "leader.elected.at.same.term.%d\n", s.currentTerm)
		// step-down to follower when it is a candidate
		if rf.state == Candidate {
			// change state to follower
			rf.setState(Follower)
		}
		// discover new leader when candidate
		// save leader name when follower
		rf.leader = req.LeaderName
	} else {
		// Update term and leader.
		rf.updateCurrentTerm(req.Term, req.LeaderName)
	}

}
func (rf *Raft) leaderLoop() {
	for 
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
			case *AppendEntriesRequest:
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
			timeoutChan = afterBetween(electionTimeout, electionTimeout * 2)
		}

	}

}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	Peers := make([]*Peer)
	for _, clientend := range peers {
		Peers = append(Peers, newPeer(rf, clientend, DefaultHeartbeatInterval))
	}
	rf.Peers = Peers
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
