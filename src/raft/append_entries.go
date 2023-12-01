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
	Term            int
	Success         bool
	ReplicatedIndex int
	LogInconsistent bool
	//prevLogIndex    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[AppendEntries] Before: Server: %v, state: %v, current Term: %v, voteFor: %v, args: %+v", rf.me, rf.currentState, rf.currentTerm, rf.voteFor, args)
	rf.checkTermOrUpdateState(args.Term)

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.LogInconsistent = false

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if rf.currentState != Follower { // Candidate -> Follower
		rf.currentState = Follower
	}

	rf.resetElectionTimeout() //重置定时器

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	prevLog := rf.logs.Get(args.PrevLogIndex)
	if prevLog == nil || prevLog.Term != args.PrevLogTerm {
		reply.Success = false
		reply.LogInconsistent = true
		return
	}

	// 3. If an existing entry conflicts with a new one(same index but different terms),
	//delete the existing entry and all that follow it
	// 4. Append any new entries not already in the log
	for i := 0; i <= args.Entries.LastIndex(); i++ {
		index := args.PrevLogIndex + i + 1
		if index > rf.logs.LastIndex() {
			rf.logs.Append(args.Entries.Get(i))
		} else if entry := rf.logs.Get(index); entry.Term != args.Entries.Get(i).Term {
			rf.logs = rf.logs[:index]
		}
	}
	// 5. If LeaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.logs.LastIndex())
	}
	reply.ReplicatedIndex = args.PrevLogIndex + len(args.Entries)
	log.Printf("[AppendEntries] After: Server: %v, state: %v, current Term: %v, voteFor: %v, args: %+v", rf.me, rf.currentState, rf.currentTerm, rf.voteFor, args)
	return
}
