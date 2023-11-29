package raft

import "log"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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
