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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type RaftState int
type Operation int

const (
	FOLLOWERSTATE RaftState = iota
	CANDIDATESTATE
	LEADERSTATE
	DEADSTATE
)
const (
	NEWTERM Operation = iota
	LEGALLEADER
	LATERCANDIDATE
	VOTEFOR
	GETVOTE
	BEDEAD
	UNDIFINE
)
const HEARTBEATS_INTERVAL = (400 + 5) * time.Millisecond
const TIMEOUT_UPPER = 900
const TIMEOUT_LOWER = 450

var r *rand.Rand

type InnerRequest struct {
	operation Operation
	term      int
	extraInf  []int
}

type InnerResponse struct {
	success   bool
	operation Operation
	term      int
	extraInf  []int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	rpcMutex   sync.Mutex // 将收到,发出rpc调用完成之后的数据处理串行化 但并发调用rpc
	stateMutex sync.Mutex // 保护raft状态的读写安全

	state             RaftState
	currentTerm       int
	votedFor          int
	votedStateOfPeers []bool
	numOfVotedPeers   int
	numOfAllPeers     int
	//用于内部数据同步
	requestChan  chan InnerRequest
	responseChan chan InnerResponse

	timer *time.Timer
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	// rf.rpcMutex.Lock()
	// defer rf.rpcMutex.Unlock()
	// tmp := rf.getCopy()
	// term = tmp.currentTerm
	// isleader = tmp.state == LEADERSTATE
	// defer fmt.Printf("------------------------------%v state is term: %v isLeader: %v\n", tmp.me, term, isleader)
	// return term, isleader

	// rf.stateMutex.Lock()
	// defer rf.rpcMutex.Unlock()
	tmp := rf.getCopy()
	term = tmp.currentTerm
	isleader = tmp.state == LEADERSTATE
	defer fmt.Printf("------------------------------%v state is term: %v isLeader: %v\n", tmp.me, term, isleader)
	return term, isleader
	// tmp := rf.getCopy()
	// term = rf.currentTerm
	// isleader = rf.state == LEADERSTATE
	// defer fmt.Printf("------------------------------%v state is term: %v isLeader: %v\n", rf.me, term, isleader)
	// return term, isleader
}

func (rf *Raft) getCopy() *Raft {
	// fmt.Printf("%v lock stateMutex in getCopy\n", rf.me)
	// defer fmt.Printf("%v unlock stateMutex in getCopy\n", rf.me)
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()
	tmp := Raft{}
	tmp.me = rf.me
	tmp.dead = rf.dead
	tmp.state = rf.state
	tmp.currentTerm = rf.currentTerm
	tmp.votedFor = rf.votedFor
	tmp.numOfAllPeers = rf.numOfAllPeers
	tmp.numOfVotedPeers = rf.numOfVotedPeers
	tmp.votedStateOfPeers = make([]bool, tmp.numOfAllPeers)
	tmp.peers = make([]*labrpc.ClientEnd, tmp.numOfAllPeers)
	// for i := range tmp.peers {
	// 	tmp.peers[i] = rf.peers[i]
	// 	tmp.votedStateOfPeers[i] = rf.votedStateOfPeers[i]
	// }
	return &tmp
}

func GetTimeoutInterval() time.Duration {
	return time.Duration(r.Intn(TIMEOUT_UPPER-TIMEOUT_LOWER)+TIMEOUT_LOWER) * time.Millisecond
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.rpcMutex.Lock()
	defer rf.rpcMutex.Unlock()

	tmp := rf.getCopy()
	fmt.Printf("%v (%v) handle RequestVote from %v\n", rf.me, tmp.currentTerm, args.CandidateId)
	reply.Term = tmp.currentTerm
	if tmp.currentTerm > args.Term {
		return
	} else {
		if tmp.currentTerm < args.Term {
			rf.requestChan <- InnerRequest{LATERCANDIDATE, args.Term, []int{args.Term, args.CandidateId}}
			ans := <-rf.responseChan
			if ans.success {
				reply.VoteGranted = true
			} else {
				reply.VoteGranted = false
			}
		} else if tmp.currentTerm == args.Term {
			rf.requestChan <- InnerRequest{VOTEFOR, args.Term, []int{args.Term, args.CandidateId}}
			ans := <-rf.responseChan
			if ans.success {
				reply.VoteGranted = true
			} else {
				reply.VoteGranted = false
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.rpcMutex.Lock()
	defer rf.rpcMutex.Unlock()

	tmp := rf.getCopy()
	fmt.Printf("%v (%v) handle AppendEntries from %v\n", tmp.me, tmp.currentTerm, args.LeaderId)
	reply.Term = tmp.currentTerm
	if tmp.currentTerm > args.Term {
		fmt.Printf("%v !!here is 1\n", rf.me)
		return
	} else {
		if tmp.currentTerm < args.Term {
			fmt.Printf("%v !!here is 2\n", rf.me)
			rf.requestChan <- InnerRequest{NEWTERM, args.Term, []int{args.Term}}
			<-rf.responseChan
		} else if tmp.currentTerm == args.Term {
			fmt.Printf("%v !!here is 3\n", rf.me)
			rf.requestChan <- InnerRequest{LEGALLEADER, args.Term, []int{args.Term, args.LeaderId}}
			<-rf.responseChan
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
	// fmt.Printf("%v (%v)send RequestVote to %v\n", rf.me, args.Term, server)
	if ok {
		rf.RequestVoteReplyHandle(server, args, reply)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	fmt.Printf("%v (%v) sends AppendEntries to %v\n", args.LeaderId, args.Term, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		fmt.Printf("%v (%v) before \n", args.LeaderId, args.Term)
		rf.AppendEntriesReplyHandle(server, args, reply)
	}
	return ok
}

func (rf *Raft) sendAllRequestVote() {
	// rf.stateMutex.Lock()
	tmp := rf.getCopy()
	// rf.stateMutex.Unlock()
	args := RequestVoteArgs{
		Term:        tmp.currentTerm,
		CandidateId: tmp.me,
	}

	for i := range tmp.peers {
		if i != rf.me && !tmp.votedStateOfPeers[i] {
			// 记得这里是go的坑点?
			tmp_i := i
			go rf.sendRequestVote(tmp_i, &args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) sendAllAppendEntries() {
	// rf.stateMutex.Lock()
	tmp := rf.getCopy()
	// rf.stateMutex.Unlock()
	args := AppendEntriesArgs{
		Term:     tmp.currentTerm,
		LeaderId: tmp.me,
	}
	for i := range tmp.peers {
		if i != rf.me {
			tmp_i := i
			go rf.sendAppendEntries(tmp_i, &args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) RequestVoteReplyHandle(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.rpcMutex.Lock()
	defer rf.rpcMutex.Unlock()
	tmp := rf.getCopy()
	fmt.Printf("%v (%v)handle RequestVoteReply from %v\n", tmp.me, tmp.currentTerm, server)
	if args.Term < reply.Term {
		if tmp.currentTerm < reply.Term {
			// NEWTERM
			fmt.Printf("server %v in RequestVoteReplyHandle\n", tmp.me)
			rf.requestChan <- InnerRequest{NEWTERM, reply.Term, []int{reply.Term}}
			<-rf.responseChan
		}
		// 当args.Term >= reply.Term时才有投票效果
	} else if args.Term >= reply.Term {
		// 只有args.Term == tmp.currentTerm 该请求才是对应该阶段的投票
		if tmp.currentTerm == args.Term && rf.state == CANDIDATESTATE {
			rf.requestChan <- InnerRequest{GETVOTE, tmp.currentTerm, []int{tmp.currentTerm, server}}
			<-rf.responseChan
		}
	}
}

func (rf *Raft) AppendEntriesReplyHandle(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.rpcMutex.Lock()
	defer rf.rpcMutex.Unlock()
	tmp := rf.getCopy()
	if args.Term < reply.Term {
		if tmp.currentTerm < reply.Term {
			rf.requestChan <- InnerRequest{NEWTERM, reply.Term, []int{reply.Term}}
			<-rf.responseChan
		}
	} else {
		// lab2A中 AppendEntriesReply除了传递Term无其他作用
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// fmt.Printf("---------Kill is called -------------------%v\n", rf.me)
	// defer fmt.Printf("---------leave kill -------------------%v\n", rf.me)
	rf.rpcMutex.Lock()
	defer rf.rpcMutex.Unlock()
	// rf.stateMutex.Lock()
	// defer rf.stateMutex.Unlock()
	// rf.state = DEADSTATE
	// fmt.Printf("1111111111111111111111111111111111111111111111111\n")
	rf.requestChan <- InnerRequest{BEDEAD, 2147483647, []int{}}
	// fmt.Printf("2222222222222222222222222222222222222222222222222\n")
	<-rf.responseChan
	// fmt.Printf("333333333333333333333333333333333333333333333333\n")
}

// func (rf *Raft) Kill() {
// 	atomic.StoreInt32(&rf.dead, 1)
// 	// Your code here, if desired.
// 	rf.mu.Lock()
// 	rf.operateChan <- pair{BEDEAD, []int{0}}
// 	rf.syncChan <- 1
// 	rf.mu.Unlock()

// }

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	// Your initialization code here (2A, 2B, 2C).
	// state             RaftState
	// currentTerm       int
	// votedFor          int
	// votedStateOfPeers []bool
	// numOfVotedPeers   int
	// numOfAllPeers     int
	// //用于内部数据同步
	// requestChan  chan InnerRequest
	// responseChan chan InnerResponse
	r = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)))
	rf.timer = time.NewTimer(GetTimeoutInterval())
	if !rf.timer.Stop() {
		<-rf.timer.C
	}
	rf.state = FOLLOWERSTATE
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.numOfAllPeers = len(peers)
	rf.numOfVotedPeers = 0
	rf.votedStateOfPeers = make([]bool, rf.numOfAllPeers)

	rf.requestChan = make(chan InnerRequest)
	rf.responseChan = make(chan InnerResponse)
	go rf.MainProcess()

	rf.stateMutex.Lock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) MainProcess() {
	fmt.Printf("%v is created\n", rf.me)
	for {
		switch rf.state {
		case FOLLOWERSTATE:
			rf.FollowerProcess()
		case CANDIDATESTATE:
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.numOfVotedPeers = 1
			for i := range rf.peers {
				if i != rf.me {
					rf.votedStateOfPeers[i] = false
				}
			}
			rf.CandidateProcess()
		case LEADERSTATE:
			// fmt.Printf("%v (%v) comes here 1\n", rf.me, rf.currentTerm)
			rf.LeaderProcess()
		case DEADSTATE:
			return
		}
	}
}

// 只有candidate会收到过时数据
// 因为在不接收外部消息时,只有follower会变成candidate,以及candidate超时currentTerm++
func (rf *Raft) FollowerProcess() {
	fmt.Printf("%v (%v) goes FollowerProcess\n", rf.me, rf.currentTerm)
	rf.timer.Reset(GetTimeoutInterval())
	// fmt.Printf("%v (%v) comes here4\n", rf.me, rf.currentTerm)
	for {
		// fmt.Printf("%v unlock stateMutex in followerProcess\n", rf.me)
		// fmt.Printf("%v (%v) comes here5\n", rf.me, rf.currentTerm)
		rf.stateMutex.Unlock()

		select {
		case <-rf.timer.C:
			// fmt.Printf("%v lock stateMutex in followerProcess\n", rf.me)
			// fmt.Printf("%v (%v) comes here2\n", rf.me, rf.currentTerm)
			rf.stateMutex.Lock()
			rf.state = CANDIDATESTATE
			// fmt.Printf("%v timeout\n", rf.me)
			return
		case tmp := <-rf.requestChan:
			// fmt.Printf("%v (%v) comes here3\n", rf.me, rf.currentTerm)
			// fmt.Printf("%v (%v) comes here xxxxxxx\n", rf.me, rf.currentTerm)
			rf.stateMutex.Lock()
			operation := tmp.operation
			term := tmp.term
			extraInf := tmp.extraInf
			if term < rf.currentTerm {
				// 接收时是不可能事件
				// 主动发送后的回复处理是可能出现的
				// fmt.Printf("不可能事件\n")
				rf.responseChan <- InnerResponse{false, UNDIFINE, rf.currentTerm, []int{}}
				continue
			}

			switch operation {
			case NEWTERM:
				// fmt.Printf("%v %v (%v) in NEWTERM", rf.currentTerm, rf.me, rf.currentTerm)
				if term <= rf.currentTerm {
					rf.responseChan <- InnerResponse{false, UNDIFINE, rf.currentTerm, []int{}}
					continue
				} else {
					if !rf.timer.Stop() {
						<-rf.timer.C
					}
					rf.state = FOLLOWERSTATE
					rf.currentTerm = extraInf[0]
					rf.votedFor = -1
					rf.responseChan <- InnerResponse{true, UNDIFINE, rf.currentTerm, []int{}}
					return
				}
			case LEGALLEADER:
				// fmt.Printf("%v %v (%v) in LEAGALLEADER\n", rf.currentTerm, rf.me, rf.currentTerm)
				if !rf.timer.Stop() {
					<-rf.timer.C
				}
				rf.state = FOLLOWERSTATE
				rf.currentTerm = extraInf[0]
				rf.responseChan <- InnerResponse{true, UNDIFINE, rf.currentTerm, []int{}}
				return
			case LATERCANDIDATE:
				// fmt.Printf("%v %v (%v) in LATERCANDIDATE\n", rf.currentTerm, rf.me, rf.currentTerm)
				if !rf.timer.Stop() {
					<-rf.timer.C
				}
				rf.state = FOLLOWERSTATE
				rf.currentTerm = extraInf[0]
				rf.votedFor = extraInf[1]
				rf.responseChan <- InnerResponse{true, UNDIFINE, rf.currentTerm, []int{}}
				return
			case VOTEFOR:
				// 有必要重新检查吗?->没必要
				// 1. Term是恒久增长的
				// 2. 处理时可能使用的Term <= currentTerm
				// 3. 小于的Term被之前丢弃
				// 4. ->只能等于
				// 投票后重新计时吗->要
				// fmt.Printf("%v %v (%v) in VOTEFOR\n", rf.currentTerm, rf.me, rf.currentTerm)
				if !rf.timer.Stop() {
					<-rf.timer.C
				}
				rf.state = FOLLOWERSTATE
				// 重新检查?
				if rf.votedFor == -1 {
					rf.votedFor = extraInf[0]
					rf.responseChan <- InnerResponse{true, UNDIFINE, rf.currentTerm, []int{}}
				} else {
					rf.responseChan <- InnerResponse{false, UNDIFINE, rf.currentTerm, []int{}}
				}
				return
			case GETVOTE:
				// fmt.Printf("%v %v (%v) in GETVOTE\n", rf.currentTerm, rf.me, rf.currentTerm)
				rf.responseChan <- InnerResponse{false, UNDIFINE, rf.currentTerm, []int{}}
				continue
			case BEDEAD:
				// fmt.Printf("%v %v (%v) in BEDEAD\n", rf.currentTerm, rf.me, rf.currentTerm)
				rf.state = DEADSTATE
				rf.responseChan <- InnerResponse{true, UNDIFINE, rf.currentTerm, []int{}}
				if !rf.timer.Stop() {
					<-rf.timer.C
				}
				return
			}
		}
		// fmt.Printf("%v (%v) comes here 6\n", rf.me, rf.currentTerm)
	}
}

//candidate 要设置多个计时器
// 在一个Term内发送多次请求吗?√
func (rf *Raft) CandidateProcess() {
	fmt.Printf("%v (%v) goes CandidateProcess\n", rf.me, rf.currentTerm)
	tmpTimer := time.NewTimer(HEARTBEATS_INTERVAL)
	rf.timer.Reset(GetTimeoutInterval())
	go rf.sendAllRequestVote()
	for {
		// fmt.Printf("%v unlock stateMutex in CandidateProcess\n", rf.me)
		rf.stateMutex.Unlock()
		select {
		case <-rf.timer.C:
			fmt.Printf("%v here is rf.timer.C\n", rf.me)
			rf.stateMutex.Lock()
			rf.state = CANDIDATESTATE
			return
		case <-tmpTimer.C:
			fmt.Printf("%v here is tmpTimer.C\n", rf.me)
			rf.stateMutex.Lock()
			tmpTimer.Reset(GetTimeoutInterval())
			go rf.sendAllRequestVote()
			continue
		case tmp := <-rf.requestChan:
			fmt.Printf("%v here is rf.requestChan\n", rf.me)
			rf.stateMutex.Lock()
			operation := tmp.operation
			term := tmp.term
			extraInf := tmp.extraInf
			if term < rf.currentTerm {
				// 接收时是不可能事件
				// 主动发送后的回复处理是可能出现的
				rf.responseChan <- InnerResponse{false, UNDIFINE, rf.currentTerm, []int{}}
				// fmt.Printf("不可能事件\n")
				continue
			}
			switch operation {
			case NEWTERM:
				if term <= rf.currentTerm {
					rf.responseChan <- InnerResponse{false, UNDIFINE, rf.currentTerm, []int{}}
					continue
				} else {
					if !rf.timer.Stop() {
						<-rf.timer.C
					}
					rf.state = FOLLOWERSTATE
					rf.currentTerm = extraInf[0]
					rf.votedFor = -1
					rf.responseChan <- InnerResponse{true, UNDIFINE, rf.currentTerm, []int{}}
					return
				}
			case LEGALLEADER:
				if !rf.timer.Stop() {
					<-rf.timer.C
				}
				rf.state = FOLLOWERSTATE
				rf.currentTerm = extraInf[0]
				rf.responseChan <- InnerResponse{true, UNDIFINE, rf.currentTerm, []int{}}
				return
			case LATERCANDIDATE:
				if !rf.timer.Stop() {
					<-rf.timer.C
				}
				rf.state = FOLLOWERSTATE
				rf.currentTerm = extraInf[0]
				rf.votedFor = extraInf[1]
				rf.responseChan <- InnerResponse{true, UNDIFINE, rf.currentTerm, []int{}}
				return
			case VOTEFOR:
				rf.responseChan <- InnerResponse{false, UNDIFINE, rf.currentTerm, []int{}}
				continue
			case GETVOTE:
				// TODO:检查重复获票 √
				if !rf.votedStateOfPeers[extraInf[1]] {
					rf.numOfVotedPeers += 1
					rf.votedStateOfPeers[extraInf[1]] = true
					if rf.numOfVotedPeers > rf.numOfAllPeers/2 {
						if !rf.timer.Stop() {
							<-rf.timer.C
						}
						rf.state = LEADERSTATE
						rf.responseChan <- InnerResponse{true, UNDIFINE, rf.currentTerm, []int{}}
						return
					} else {
						continue
					}
				} else {
					continue
				}

			case BEDEAD:
				rf.state = DEADSTATE
				rf.responseChan <- InnerResponse{true, UNDIFINE, rf.currentTerm, []int{}}
				if !rf.timer.Stop() {
					<-rf.timer.C
				}
				return
			}
		}
	}
}

func (rf *Raft) LeaderProcess() {
	fmt.Printf("%v (%v) goes LeaderProcess\n", rf.me, rf.currentTerm)
	rf.timer.Reset(HEARTBEATS_INTERVAL)
	go rf.sendAllAppendEntries()
	for {
		rf.stateMutex.Unlock()
		select {
		case <-rf.timer.C:
			rf.stateMutex.Lock()
			rf.state = LEADERSTATE
			return
		case tmp := <-rf.requestChan:
			rf.stateMutex.Lock()
			operation := tmp.operation
			term := tmp.term
			extraInf := tmp.extraInf
			if term < rf.currentTerm {
				// 接收时是不可能事件
				// 主动发送后的回复处理是可能出现的
				rf.responseChan <- InnerResponse{false, UNDIFINE, rf.currentTerm, []int{}}
				// fmt.Printf("不可能事件\n")
				continue
			}
			switch operation {
			case NEWTERM:
				if term <= rf.currentTerm {
					rf.responseChan <- InnerResponse{false, UNDIFINE, rf.currentTerm, []int{}}
					continue
				} else {
					if !rf.timer.Stop() {
						<-rf.timer.C
					}
					rf.state = FOLLOWERSTATE
					fmt.Printf("%v server", rf.me)
					rf.currentTerm = extraInf[0]
					rf.votedFor = -1
					rf.responseChan <- InnerResponse{true, UNDIFINE, rf.currentTerm, []int{}}
					return
				}
				// Leader以及Follower使用以下被注释代码就可行
				// 因为handle函数是串行执行的,在执行handle函数时,raft的状态有且仅有follower和candidate超时事件能改变
				// 而该超时事件将raft变为candidate
				// !!!!!!所以follower和leader无需检查传入的tmp.Term是否过期!!!!!!!
				// !!!
				// if !rf.timer.Stop() {
				// 	<-rf.timer.C
				// }
				// rf.state = FOLLOWERSTATE
				// rf.currentTerm = extraInf[0]
				// rf.votedFor = -1
				// rf.responseChan <- InnerResponse{true, UNDIFINE, rf.currentTerm, []int{}}
				// return
			case LEGALLEADER:
				if !rf.timer.Stop() {
					<-rf.timer.C
				}
				rf.state = FOLLOWERSTATE
				rf.currentTerm = extraInf[0]
				rf.responseChan <- InnerResponse{true, UNDIFINE, rf.currentTerm, []int{}}
				return
			case LATERCANDIDATE:
				if !rf.timer.Stop() {
					<-rf.timer.C
				}
				rf.state = FOLLOWERSTATE
				rf.currentTerm = extraInf[0]
				rf.votedFor = extraInf[1]
				rf.responseChan <- InnerResponse{true, UNDIFINE, rf.currentTerm, []int{}}
				return
			case VOTEFOR:
				rf.responseChan <- InnerResponse{false, UNDIFINE, rf.currentTerm, []int{}}
				continue
			case GETVOTE:
				rf.responseChan <- InnerResponse{false, UNDIFINE, rf.currentTerm, []int{}}
				continue
			case BEDEAD:
				rf.state = DEADSTATE
				rf.responseChan <- InnerResponse{true, UNDIFINE, rf.currentTerm, []int{}}
				if !rf.timer.Stop() {
					<-rf.timer.C
				}
				return
			}
		}
	}
}
