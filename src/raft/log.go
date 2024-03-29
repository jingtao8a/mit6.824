package raft

import "fmt"

type LogEntry struct {
	Command interface{}
	Term    int
}

func (l *LogEntry) String() string {
	return fmt.Sprintf("{Term:%v, Command:%v}", l.Term, l.Command)
}

type LogEntries []*LogEntry

func (le *LogEntries) LastIndex() int { return len((*le)) - 1 }

func (le *LogEntries) Get(i int) *LogEntry {
	if i <= le.LastIndex() {
		return (*le)[i]
	}
	return nil
}

func (le *LogEntries) Append(entry *LogEntry) {
	(*le) = append((*le), entry)
}

func (le *LogEntries) GetLast() *LogEntry {
	return le.Get(le.LastIndex())
}
