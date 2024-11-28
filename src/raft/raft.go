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
	//	"bytes"

	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	votedFor    int
	log         []Entry

	state State

	//volatile state on all servers
	commitIndex      int
	lastAppliedIndex int

	//volatile state on all leaders
	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg

	winElectCh  chan bool
	stepDownCh  chan bool
	grantVoteCh chan bool
	heartbeatCh chan bool
}

type State string

const (
	Follower  State = "follower"
	Leader    State = "leader"
	Candidate State = "candidate"
)

const ElectionTimeoutMs = 2000 //ms
const HeartbeatTimeout = 300   //ms

type Entry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil || e.Encode(rf.log) != nil {
		panic("failed to encode raft persistent state")
	}
	data := w.Bytes()
	rf.persister.Save(data, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("ERRROR")
	} else {
		rf.currentTerm = currentTerm
		rf.log = log
		rf.votedFor = votedFor
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (3A, 3B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (3A).
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	Entries           []Entry
	PrevLogIndex      int
	PrevLogTerm       int
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // term in the conflicting entry (if any)
	XIndex  int // index of first entry with that term (if any)
	XLen    int // log length
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	fmt.Println("received REQUEST VOTE", rf.me, "from", args.CandidateId, "with last log index", args.LastLogIndex)
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		fmt.Println(rf.me, "converting to follower", args.Term, rf.currentTerm)
		rf.convertToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	fmt.Println("me", rf.me, "voted for", rf.votedFor)
	alreadyVoted := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	if alreadyVoted && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		fmt.Println("voting for ", args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.sendToChannel(rf.grantVoteCh, true)
		return
	}
	// Your code here (3A, 3B).
}

// Raft determines which of two logs is more up-to-date
// //by comparing the index and term of the last entries in the
// logs. If the logs have last entries with different terms, then
// the log with the later term is more up-to-date. If the logs
// end with the same term, then whichever log is longer is
// more up-to-date
func (rf *Raft) isLogUpToDate(candidateLastLogIndex int, candidateLastLogTerm int) bool {
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	if candidateLastLogTerm != lastLogTerm {
		return candidateLastLogTerm > lastLogTerm
	}
	return candidateLastLogIndex >= lastLogIndex
}

func (rf *Raft) convertToFollower(term int) {
	state := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	// step down if not follower, this check is needed
	// to prevent race where state is already follower
	if state != Follower {
		rf.sendToChannel(rf.stepDownCh, true)
	}

}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	fmt.Println("I", rf.me, rf.currentTerm, "received appendEntries from someone who thinks its term", args.Term, rf.state, "with log", args.Entries, args.PrevLogIndex, args.PrevLogTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// node is a candidate and finds another candidate with a higher term
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	rf.sendToChannel(rf.heartbeatCh, true)
	lastIndex := len(rf.log) - 1
	if args.PrevLogIndex > lastIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		// reply.XIndex = lastIndex
		return
	}

	entry := rf.log[args.PrevLogIndex]
	if entry.Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		// reply.XTerm = entry.Term
		return
	}

	fmt.Println("my", rf.me, "logs are before", rf.log)
	conflictIndex, isConflict := findConflictingEntries(rf.log, args.Entries, args.PrevLogIndex)
	if isConflict {
		fmt.Println("found confflict", conflictIndex, rf.log, args.Entries)
		rf.log = rf.log[:conflictIndex]
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	if len(rf.log) > args.PrevLogIndex+1 {
		overwriteOwnLogs := rf.log[:args.PrevLogIndex+1]
		rf.log = append(overwriteOwnLogs, args.Entries...)
	} else {
		rf.log = append(rf.log, args.Entries...)
	}
	fmt.Println("my", rf.me, "logs are now", rf.log)
	fmt.Println("my", rf.me, "leader", args.LeaderCommitIndex, rf.commitIndex)
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommitIndex), float64(len(rf.log)-1)))
		go rf.sendApplyMsgs()
	}

}

func findConflictingEntries(log []Entry, leaderLogs []Entry, prevLogIndex int) (int, bool) {
	if len(leaderLogs) == 0 || prevLogIndex == 0 {
		return 0, false
	}
	for i := prevLogIndex + 1; i < len(log); i++ {
		existingEntry := log[i]
		newEntry := leaderLogs[i-prevLogIndex-1]

		isConflict := existingEntry.Term != newEntry.Term
		if isConflict {
			return i, true
		}
	}

	return 0, false

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		fmt.Println("APPEND ENTRIES FAILED FOR", server)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		rf.persist()
		fmt.Println("I", rf.me, "have been demoted to follower")
		return
	}

	if reply.Success {
		fmt.Println(rf.me, "received success response from", server)
		//update
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		fmt.Println("new matchindex", newMatchIndex, "old", rf.matchIndex[server])
		if newMatchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatchIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		for n := len(rf.log) - 1; n >= rf.commitIndex; n-- {
			count := 1
			if rf.log[n].Term == rf.currentTerm {
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me && rf.matchIndex[i] >= n {
						count++
					}
				}
			}
			majority := math.Floor((float64(len(rf.peers) / 2)) + 1)
			if count >= int(majority) {
				rf.commitIndex = n
				fmt.Println("commitindex is now", rf.commitIndex)
				go rf.sendApplyMsgs()
				break
			}
		}
	} else {
		fmt.Println(rf.me, "FAILED, decrementing nextindex for ", server)
		rf.nextIndex[server] = rf.nextIndex[server] - 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	}

	//failed
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
	_, isLeader = rf.GetState()
	if !isLeader || rf.killed() {
		return -1, -1, false
	}

	//append log
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: command})
	term = rf.currentTerm
	index = len(rf.log)
	rf.persist()

	return index - 1, term, true
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Leader:
			select {
			case <-rf.stepDownCh:
			case <-time.After(120 * time.Millisecond):
				rf.mu.Lock()
				rf.sendLogs()
				rf.mu.Unlock()
			}

		case Follower:
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartbeatCh:
			case <-time.After(time.Duration(360+rand.Intn(240)) * time.Millisecond):
				rf.startElection(rf.state)
			}
		case Candidate:
			select {
			case <-rf.stepDownCh:
			case <-rf.winElectCh:
				fmt.Println("CONVERTING TO ELADER")
				rf.convertToLeader()
			case <-time.After(time.Duration(360+rand.Intn(240)) * time.Millisecond):
				rf.startElection(rf.state)
			}

			// Your code here (3A)
			// Check if a leader election should be started.

			//if the election timeout window has passed and nothing happens, start election.
			//so we need to reset the election timeout on every communcation

			// pause for a random amount of time between 50 and 350
			// milliseconds.

		}
	}
}

func (rf *Raft) sendLogs() {
	fmt.Println(rf.me, "sending logs", rf.log, "term", rf.currentTerm)
	if rf.state != Leader {
		return
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		entriesToSend := rf.log[rf.nextIndex[i]:]
		fmt.Println(rf.me, rf.state, "sending to", i, rf.nextIndex[i], entriesToSend)
		req := &AppendEntriesArgs{
			Term:              rf.currentTerm,
			Entries:           entriesToSend,
			LeaderId:          rf.me,
			LeaderCommitIndex: rf.commitIndex,
			PrevLogIndex:      rf.nextIndex[i] - 1,
		}
		req.PrevLogTerm = rf.log[req.PrevLogIndex].Term
		reply := &AppendEntriesReply{}

		go rf.sendAppendEntries(i, req, reply)
	}
}

func (rf *Raft) sendApplyMsgs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastAppliedIndex + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		fmt.Println("I", rf.me, "applied message", i, rf.log[i].Command)
		rf.lastAppliedIndex = i
	}

}

func (rf *Raft) resetChannels() {
	rf.winElectCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// this check is needed to prevent race
	// while waiting on multiple channels
	if rf.state != Candidate {
		return
	}

	rf.resetChannels()
	rf.state = Leader
	nextIndex := make([]int, len(rf.peers))
	for idx := range rf.peers {
		nextIndex[idx] = len(rf.log)
	}
	rf.nextIndex = nextIndex
	rf.matchIndex = make([]int, len(rf.peers))
	rf.sendLogs()
}

func (rf *Raft) convertToCandidate() {
	rf.resetChannels()
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.persist()
	fmt.Println(rf.me, "I am starting to be a candaidate", rf.state, rf.currentTerm)
}

func (rf *Raft) startElection(state State) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if state != rf.state {
		return
	}

	rf.convertToCandidate()
	if rf.state != Candidate {
		return
	}

	voteCount := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			var lastLogTerm = 0
			lastLogTerm = rf.log[len(rf.log)-1].Term
			req := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  lastLogTerm,
			}

			var reply RequestVoteReply
			ok := rf.sendRequestVote(i, req, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				fmt.Println(rf.me, "SOMEONE HAS HIGHER TERM THAN ME", rf.currentTerm, reply.Term)
				rf.convertToFollower(reply.Term)
				rf.persist()
				return
			}

			if reply.VoteGranted {
				voteCount++
				majority := math.Floor((float64(len(rf.peers) / 2)) + 1)
				if voteCount >= int(majority) {
					rf.sendToChannel(rf.winElectCh, true)
				}
			}
		}(i)
	}
}

func (rf *Raft) sendToChannel(ch chan bool, value bool) {
	select {
	case ch <- value:
	default:
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
	rf.persister = persister
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.commitIndex = 0
	rf.lastAppliedIndex = 0
	rf.applyCh = applyCh
	rf.winElectCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
