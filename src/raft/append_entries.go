package raft

import "log"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      LogEntries
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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
