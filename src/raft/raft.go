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
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

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
type ServerState int

const (
	Leader    ServerState = 0
	Follower  ServerState = 1
	Candidate ServerState = 2
)

const (
	heartbeatInterval         = 120 * time.Millisecond
	electionTimeoutLowerBound = 400
	electionTimeoutUpperBound = 600
	rpcTimeoutLimit           = 1000 * time.Millisecond
)

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
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all servers
	currentTerm int
	voteFor     int
	//log         []int

	//Volatile state on all servers
	//正常情况下commitIndex和lastApplied应该是一样的，但是如果有一个新的提交，并且还未应用的话lastApplied应该要更小些
	//commitIndex int //状态机中已知的被提交的日志条目的索引值（初始化为0，持续递增）
	//lastApplied int //最后一个被追加到状态机日志的索引值
	//
	////Volatile state on leaders
	//nextIndex  []int //对于每一个server，需要发送给他下一个日志条目的索引值
	//matchIndex []int //对于每一个server，已经复制给该server的最后日志条目下标

	//Additional properties
	currentState      ServerState
	electionTimeoutAt time.Time
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	//PrevLogIndex int
	//PrevLogTerm  int
	//Entries      []int
	//LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState == Leader {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
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
	//LastLogIndex int
	//LastLogTerm  int
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[RequestVote] Start: Server: %v, state: %v, current Term: %v, voteFor: %v, args: %+v", rf.me, rf.currentState, rf.currentTerm, rf.voteFor, args)
	rf.checkTermOrUpdateState(args.Term)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
	}
	rf.resetElectionTimeout() //重置定时器
	log.Printf("[RequestVote] Finish: Server: %v, state: %v, current Term: %v, voteFor: %v, args: %+v", rf.me, rf.currentState, rf.currentTerm, rf.voteFor, args)
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[AppendEntries] Before: Server: %v, state: %v, current Term: %v, voteFor: %v, args: %+v", rf.me, rf.currentState, rf.currentTerm, rf.voteFor, args)
	rf.checkTermOrUpdateState(args.Term)

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}
	if rf.currentState != Follower { // Candidate
		rf.currentState = Follower
	}
	reply.Success = true
	rf.resetElectionTimeout() //重置定时器
	log.Printf("[AppendEntries] After: Server: %v, state: %v, current Term: %v, voteFor: %v, args: %+v", rf.me, rf.currentState, rf.currentTerm, rf.voteFor, args)
	return
}

func (rf *Raft) checkTermOrUpdateState(term int) {
	if term > rf.currentTerm {
		rf.voteFor = -1
		rf.currentState = Follower
		rf.currentTerm = term
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
	rf.mu.Lock()
	rf.checkTermOrUpdateState(reply.Term)
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	rf.checkTermOrUpdateState(reply.Term)
	rf.mu.Unlock()
	return ok
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//检查raft定时器是否超时
func (rf *Raft) isElectionTimeout() bool {
	if time.Now().After(rf.electionTimeoutAt) {
		log.Printf("Server %v is election timeout, state %v, term %v, votedFor %v", rf.me, rf.currentState, rf.currentTerm, rf.voteFor)
		return true
	}
	return false
}

//重置raft定时器
func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeoutAt = time.Now().Add(
		time.Millisecond*time.Duration(rand.Intn(electionTimeoutUpperBound-electionTimeoutLowerBound)) + electionTimeoutLowerBound)
}

func (rf *Raft) tryConvertToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("[tryConvertToCandidate] Server %v, state: %v, term: %v, voteFor: %v", rf.me, rf.currentState, rf.currentTerm, rf.voteFor)
	if rf.isElectionTimeout() {
		rf.currentState = Candidate
	}
}

func (rf *Raft) electLeader() {
	rf.mu.Lock()
	//1. Increase currentTerm
	rf.currentTerm++
	//2. Vote for self
	rf.voteFor = rf.me
	//3. Reset election timer
	rf.resetElectionTimeout()
	rf.mu.Unlock()
	//4. Send RequestVote RPC to other servers
	log.Printf("[electLeader] Server %v start election, state: %v, term: %v", rf.me, rf.currentState, rf.currentTerm)
	successVoteNums := len(rf.peers)/2 + 1
	voteNums := 1
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	go func() {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(serverID int) {
				reply := &RequestVoteReply{}
				log.Printf("[electLeader] Server %v sendRequestVote to server %d, args: %+v", rf.me, serverID, args)
				rf.sendRequestVote(serverID, args, reply)
				if reply.VoteGranted {
					rf.mu.Lock()
					voteNums++
					log.Printf("[electLeader] Server %v received vote in term %v, currentVoteNums: %v, successVoteNums: %v", rf.me, rf.currentTerm, voteNums, successVoteNums)
					rf.mu.Unlock()
				}
			}(i)
		}
	}()

	for {
		time.Sleep(10 * time.Millisecond)
		isFinished := false
		//5. Stop election if:
		//  5.1. Election timeout
		//  5.2. Receive the most of votes from other servers
		//  5.3. Once received RPC AppendEntry from leader, become follower
		rf.mu.Lock()
		if rf.currentState == Candidate && voteNums >= successVoteNums { // 5.2
			log.Printf("[electLeader] Server %v received the most vote, election success and become leader in term %v", rf.me, rf.currentTerm)
			rf.currentState = Leader
			isFinished = true
		} else if rf.currentState == Follower { // 5.3
			log.Printf("[electLeader] Server %v received the heartbeat from other server, stop election and become follower in term %v", rf.me, rf.currentTerm)
			isFinished = true
		} else if rf.isElectionTimeout() { // 5.1
			log.Printf("[electLeader] Server %v election timeout in term %v", rf.me, rf.currentTerm)
			rf.currentState = Follower
			isFinished = true
		}
		rf.mu.Unlock()

		if isFinished {
			break
		}
	}
}

func (rf *Raft) sendHeartbeat() {
	time.Sleep(heartbeatInterval)

	heartbeatNums := 1
	heartbeatSuccessLimit := len(rf.peers)/2 + 1
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}

	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers) - 1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(serverID int) {
			defer wg.Done()
			reply := &AppendEntriesReply{}
			ch := make(chan struct{}, 1)
			go func() {
				rf.sendAppendEntries(serverID, args, reply)
				ch <- struct{}{}
			}()
			select {
			case <-ch:
				rf.mu.Lock()
				heartbeatNums++
				rf.mu.Unlock()
				log.Println("finished")
			case <-time.After(rpcTimeoutLimit):
				log.Println("timeout")
			}
		}(i)
	}

	wg.Wait()
	if heartbeatNums < heartbeatSuccessLimit {
		rf.mu.Lock()
		rf.currentState = Follower
		rf.mu.Unlock()
	}
}

func (rf *Raft) run() {
	for {
		time.Sleep(10 * time.Millisecond) //？？？？？？？？
		switch rf.currentState {
		case Follower:
			rf.tryConvertToCandidate()
			break
		case Candidate:
			rf.electLeader()
			break
		case Leader:
			rf.sendHeartbeat()
			break
		default:
			log.Fatal("wrong ServerState")
		}
	}
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
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 1
	rf.voteFor = -1

	rf.currentState = Follower
	rf.resetElectionTimeout() //重置定时器

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}
