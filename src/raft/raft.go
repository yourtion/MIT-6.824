package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
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

// 日志条目
type logInfo struct {
	Term int    // 收到时的任期号
	Info string // 用户状态机执行的指令
}

// RoleType 角色类型
type RoleType uint8

const (
	RoleLeader    RoleType = 0 // Leader
	RoleFollower  RoleType = 1 // Follower
	RoleCandidate RoleType = 2 // Candidate
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role         RoleType  // 当前角色
	currentTerm  int       // 服务器最后一次知道的任期号（初始化为 0，持续递增）
	votedFor     int       // 在当前获得选票的候选人的 Id
	log          []logInfo // 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号
	lastLogIndex int       // 最后日志信息
	lastLogTerm  int       // 最后日志信息
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// TODO: 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == RoleLeader
}

// save Raft's persistent state to stable storage, where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// TODO: 2C
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// TODO: 2C
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

// example RequestVote RPC arguments structure. field names must start with capital letters!
type RequestVoteArgs struct {
	// TODO: 2A data
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// TODO: 2B data
}

// example RequestVote RPC reply structure. field names must start with capital letters!
type RequestVoteReply struct {
	// TODO: 2A data
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// TODO: 2A
	// 如果term < currentTerm返回 false
	if args.Term < rf.currentTerm {
		return
	}
	// 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他
	DPrintf("RequestVote Status: %+v", rf)
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}
	if args.LastLogIndex >= rf.lastLogIndex {
		reply.VoteGranted = true
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.role = RoleFollower
	}
	DPrintf("RequestVote Status After: %+v", rf)
	// TODO: 2B
}

func (rf *Raft) StartVoteRequest() {
	// 准备投票数据
	rf.mu.Lock()
	term := rf.currentTerm + 1
	me := rf.me
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm:  rf.lastLogTerm,
	}
	rf.role = RoleCandidate
	rf.mu.Unlock()
	// 开始发起投票
	voteCount := 1
	for i, _ := range rf.peers {
		if i == me {
			continue
		}
		reply := RequestVoteReply{Term: term}
		rf.sendRequestVote(i, &args, &reply)
		DPrintf("RequestVoteReply[%d]: %+v", voteCount, reply)
		if reply.VoteGranted {
			rf.mu.Lock()
			voteCount += 1
			if voteCount >= len(rf.peers)/2+1 && rf.role == RoleCandidate {
				rf.currentTerm = term
				rf.role = RoleLeader
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		}
	}
}

type AppendEntriesArgs struct{}

type AppendEntriesReply struct{}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

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
	DPrintf("sendRequestVote: %d %+v %+v", server, args, reply)
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
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

	// TODO: 2B

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
	// TODO
}

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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// TODO: 2A initialization code
	rf.votedFor = -1
	rf.role = RoleFollower
	// TODO: 2B initialization code
	// TODO: 2C initialization code

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startLoop()

	return rf
}

func (rf *Raft) startLoop() {
	for {
		time.Sleep(time.Millisecond * time.Duration(rand.Int31n(100)))
		if rf.role == RoleFollower && rf.votedFor == -1 {
			rf.StartVoteRequest()
		}
	}
}
