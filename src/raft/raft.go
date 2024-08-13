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

	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	votedFor    *int
	log         []Entry

	state               State
	latestCommunication time.Time //with any peer

	//volatile state on all servers
	commitIndex      int
	lastAppliedIndex int

	//volatile state on all leaders
	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Println("received", rf.me, "from", args.CandidateId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		fmt.Println(rf.me, "converting to follower", args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.votedFor = nil
		//convert to follower
		rf.state = Follower
	}

	if args.LastLogIndex > 0 {
		uptoDateLog := len(rf.log)-1 == args.LastLogIndex && rf.log[len(rf.log)-1].Term == args.LastLogTerm
		if !uptoDateLog {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	}

	fmt.Println("me", rf.me, "voted for", rf.votedFor)
	alreadyVoted := rf.votedFor == nil || rf.votedFor == &args.CandidateId
	if alreadyVoted {
		fmt.Println("voting for ", args.CandidateId)
		rf.votedFor = &args.CandidateId
		rf.latestCommunication = time.Now()
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
	// Your code here (3A, 3B).
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Println("received appendEntries", rf.me, rf.state, "with log", args.Entries, args.PrevLogIndex, args.PrevLogTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// node is a candidate and finds another candidate with a higher term
	if rf.state == Candidate && args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.latestCommunication = time.Now()
	isHeartbeat := len(args.Entries) == 0
	if isHeartbeat {
		fmt.Println("received heartbeat", rf.me, rf.state)
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}

	if len(rf.log) > args.PrevLogIndex {
		entry := rf.log[args.PrevLogIndex]
		if entry.Term != args.PrevLogTerm {
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
	}

	conflictIndex, isConflict := findConflictingEntries(rf.log, args.Entries)
	if isConflict {
		//delete logs conflictIndex and later
		rf.log = rf.log[:conflictIndex]
	}

	fmt.Println("my", rf.me, "logs are before", rf.log)
	rf.log = append(rf.log, args.Entries...)
	fmt.Println("my", rf.me, "logs are now", rf.log)
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommitIndex), float64(len(rf.log)-1)))
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	rf.sendApplyMsgs()

	return
	// Your code here (3A, 3B).
}

func findConflictingEntries(log []Entry, leaderLogs []Entry) (int, bool) {
	i := 0
	for i < len(log) && i < len(leaderLogs) {
		time.Sleep(10 * time.Millisecond)
		existingEntry := log[i]
		newEntry := leaderLogs[i]

		isConflict := existingEntry.Term != newEntry.Term
		if isConflict {
			return i, true
		}
		i++
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	term, isLeader = rf.GetState()
	if !isLeader || rf.killed() {
		return -1, -1, false
	}

	//append log
	rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: command})
	term = rf.currentTerm
	index = len(rf.log)

	return index, term, isLeader
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
		_, isLeader := rf.GetState()
		if isLeader {
			newLogsToBeApplied := len(rf.log) > int(math.Max(float64(rf.commitIndex-1), 0))
			if newLogsToBeApplied {
				rf.sendLogs()
			} else {
				rf.sendHeartbeats()
			}
			//send heartbeats
			time.Sleep(time.Millisecond * HeartbeatTimeout)
			continue
		}

		// Your code here (3A)
		// Check if a leader election should be started.

		//if the election timeout window has passed and nothing happens, start election.
		//so we need to reset the election timeout on every communcation
		if time.Since(rf.latestCommunication).Milliseconds() > ElectionTimeoutMs {
			nodeBecameLeader := rf.startElection()
			if nodeBecameLeader {
				rf.sendHeartbeats()
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.3
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendLogs() {
	successCount := 0
	responseCount := 0
	fmt.Println(rf.me, "sending logs", rf.log)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[i]
		if len(rf.log) < nextIndex {
			fmt.Println(nextIndex, rf.log)
			continue
		}
		go func(i int) {
			nextIndex := rf.nextIndex[i]
			prevLogIndex := int(math.Max(float64(nextIndex-1), 0))
			prevLogTerm := rf.log[prevLogIndex].Term

			entriesToSend := rf.log[nextIndex:]
			fmt.Println("sending ", prevLogIndex, nextIndex, entriesToSend)
			req := &AppendEntriesArgs{
				Term:              rf.currentTerm,
				Entries:           entriesToSend,
				LeaderId:          rf.me,
				LeaderCommitIndex: rf.commitIndex,
				PrevLogIndex:      prevLogIndex,
				PrevLogTerm:       prevLogTerm,
			}
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, req, reply)
			if !ok {
				return
			}
			responseCount++

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
			}

			if reply.Success {
				fmt.Println(rf.me, "received success response from", i, (prevLogIndex)+len(req.Entries))
				//update

				rf.nextIndex[i] = nextIndex + len(req.Entries)
				rf.matchIndex[i] = prevLogIndex + len(req.Entries) - 1
				successCount++
				return
			}

			//failed
			rf.nextIndex[i] = len(req.Entries) - 1
		}(i)
	}

	time.Sleep(500 * time.Millisecond)
	majority := math.Floor((float64(responseCount / 2)) + 1)
	if float64(successCount) >= majority {
		rf.sendApplyMsgs()
	}
}

func (rf *Raft) sendApplyMsgs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.lastAppliedIndex < len(rf.log) {
		rf.lastAppliedIndex++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastAppliedIndex-1].Command,
			CommandIndex: rf.lastAppliedIndex,
		}
		rf.applyCh <- applyMsg
		rf.commitIndex = rf.lastAppliedIndex
		fmt.Println(rf.me, "sent applymsg", applyMsg.CommandIndex)
	}

}

func findPrevLogIndexAndTerm(lastAppliedIndex int, log []Entry) (int, int) {
	if lastAppliedIndex > 0 {
		lastAppliedIndex = lastAppliedIndex - 1
	}
	prevLogIndex := lastAppliedIndex
	prevLogTerm := log[prevLogIndex].Term
	fmt.Println(prevLogIndex, lastAppliedIndex, log)
	return prevLogIndex, prevLogTerm
}

func (rf *Raft) sendHeartbeats() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			req := &AppendEntriesArgs{
				Term:    rf.currentTerm,
				Entries: []Entry{},
			}
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, req, reply)
			if !ok {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
			}
		}(i)
	}
}

func (rf *Raft) startElection() bool {
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = &rf.me
	rf.state = Candidate
	rf.latestCommunication = time.Now()
	fmt.Println(rf.me, "I am starting to be a candaidate", rf.state, rf.currentTerm)

	voteCount := 0.0
	responseCount := 0.0
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			var lastLogTerm = 0
			if len(rf.log) > 0 {
				lastLogTerm = rf.log[len(rf.log)-1].Term
			}
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

			responseCount++

			if reply.Term > rf.currentTerm {
				fmt.Println(rf.me, "term", rf.currentTerm, reply.Term)
				rf.currentTerm = reply.Term
				rf.state = Follower
				return
			}

			if reply.VoteGranted {
				voteCount++
			}
		}(i)
	}

	time.Sleep(1 * time.Second) //hopefully I have received all replies at this point. Not sure what else the timeout should be

	majority := math.Floor((responseCount / 2) + 1)
	fmt.Println("votecount", voteCount, "for", rf.me, "in state", rf.state, "and I need ", majority, "votes")
	if voteCount >= majority {
		if rf.state == Candidate {
			fmt.Println("FOUND LEADER FOUND LEADER")
			rf.state = Leader

			nextIndex := make([]int, len(rf.peers))
			for idx := range rf.peers {
				nextIndex[idx] = len(rf.log)
			}
			rf.nextIndex = nextIndex
			rf.matchIndex = make([]int, len(rf.peers))
		} else {
			panic("state has to be candidate")
		}
	}

	return rf.state == Leader
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.latestCommunication = time.Now()
	rf.log = make([]Entry, 0)
	rf.commitIndex = 0
	rf.lastAppliedIndex = 0
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
