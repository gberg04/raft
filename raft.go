package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"bytes"
	"cs350/labgob"
	"cs350/labrpc"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // This peer's index into peers[]
	dead            int32               // Set by Kill()
	currentTerm     int
	votedFor        int
	log             []*Entry
	state           Status
	commitIndex     int
	lastApplied     int
	lastLogIndex    int
	nextIndex       []int
	matchIndex      []int
	heartbeats      int
	electionTimeout time.Time
	candVotes       int
	electLeader     chan bool
	countingVotes   bool
	applyMsg        chan ApplyMsg

	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type Entry struct {
	//Index    int
	Action   interface{}
	TermSeen int
}

type Status string

var (
	follower  Status = "follower"
	candidate Status = "candidate"
	leader    Status = "leader"
)

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (4A).

	rf.mu.Lock()

	term = rf.currentTerm
	if rf.state == leader {
		isleader = true
	} else {
		isleader = false
	}

	rf.mu.Unlock()

	return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4B).
	// Example:

	rf.mu.Lock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	currentTerm := 0
	votedFor := 0

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&rf.log) != nil {

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastLogIndex = len(rf.log) - 1
	}

	rf.mu.Unlock()
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	CandTerm     int
	CandID       int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (4A, 4B).
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	VoteGranted bool
	Term        int
	// Your data here (4A).
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()

	if rf.currentTerm == args.CandTerm && rf.votedFor == args.CandID {
		rf.resetTimeout()
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.persist()
		rf.mu.Unlock()
		return
	}

	rf.BecameFollower(args.CandTerm)

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.CandTerm < reply.Term || (rf.currentTerm == args.CandTerm && rf.votedFor != -1) {
		rf.mu.Unlock()
		return
	}

	var last int
	var length int

	if len(rf.log) == 0 {
		last = -1
		length = -1
	} else {
		last = rf.log[len(rf.log)-1].TermSeen
		length = len(rf.log) - 1
	}

	upToDate := (args.LastLogTerm > last || args.LastLogTerm == last && args.LastLogIndex >= length)

	if upToDate {

		//fmt.Printf("%d voted for %d for term %d \n", rf.me, args.CandID, args.CandTerm)
		rf.votedFor = args.CandID
		reply.VoteGranted = true
		//rf.state = follower
		rf.resetTimeout()
	}

	rf.persist()

	rf.mu.Unlock()

}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
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
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	LeadTerm     int
	LeadId       int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeadCommit   int
	Heartbeat    bool
}

type AppendEntriesReply struct {
	Success       bool
	NewTerm       int
	LatestTerm    int
	LatestIndex   int
	LastestLength int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()

	log.Printf("%d recieved appendEntries from %d for term %d with entries ", rf.me, args.LeadId, args.LeadTerm)
	rf.PrintLog(args.Entries)
	log.Printf("\n")

	rf.BecameFollower(args.LeadTerm)

	rf.mu.Unlock()

	rf.mu.Lock()

	reply.LastestLength = len(rf.log)
	reply.LatestIndex = -1
	reply.LatestTerm = -1
	reply.NewTerm = rf.currentTerm
	reply.Success = false

	//fmt.Printf("%d recieved append request from %d for entries %p \n", rf.me, args.LeadId, args.Entries)

	if args.LeadTerm < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if len(rf.log) < args.PrevLogIndex+1 {
		rf.mu.Unlock()
		return
	}

	if args.PrevLogIndex != -1 && rf.log[args.PrevLogIndex].TermSeen != args.PrevLogTerm {
		reply.LatestTerm = rf.log[args.PrevLogIndex].TermSeen
		for i, j := range rf.log {
			if j.TermSeen == reply.LatestTerm {
				reply.LatestIndex = i
				break
			}
		}
		rf.mu.Unlock()
		return
	}

	//fmt.Printf("%d has commitIndex %d \n", rf.me, rf.commitIndex)

	index := 0

	for ; index < len(args.Entries); index++ {

		applyIndex := args.PrevLogIndex + 1 + index

		if applyIndex > len(rf.log)-1 {
			break
		}

		if rf.log[applyIndex].TermSeen != args.Entries[index].TermSeen {

			rf.log = rf.log[:applyIndex]
			rf.lastLogIndex = len(rf.log) - 1
			rf.persist()
			break

		}
	}

	reply.Success = true
	rf.resetTimeout()

	if len(args.Entries) > 0 {

		log.Printf("%d appending entries ", rf.me)
		rf.PrintLog(args.Entries)
		log.Printf(" to log \n")

		rf.log = append(rf.log, args.Entries[index:]...)
		rf.lastLogIndex = len(rf.log) - 1
		rf.persist()

		//fmt.Printf("%d current log state: ", rf.me)
		//rf.PrintLog(rf.log)
		//fmt.Printf("\n")
		//fmt.Printf("%d current commit index: %d \n", rf.me, rf.commitIndex)

	}

	if rf.commitIndex < args.LeadCommit {

		commitTo := min(args.LeadCommit, rf.lastLogIndex)
		//fmt.Println("LeadCommit: ", args.LeadCommit)
		//fmt.Println("lastLogIndex: ", rf.lastLogIndex)
		//fmt.Print("Current Log: ")
		//rf.PrintLog(rf.log)
		//fmt.Print("\n")

		for committing := rf.commitIndex + 1; committing <= commitTo; committing++ {

			rf.commitIndex = committing

			log.Printf("%d committing %v to log from %d \n", rf.me, rf.log[committing].Action, args.LeadId)

			apply := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[committing].Action,
				CommandIndex: committing,
			}

			rf.applyMsg <- apply

		}

	}

	rf.persist()

	//reply.Success = true

	rf.mu.Unlock()

}

func (rf *Raft) sendAppendEntries(index int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	/*
		if args.Heartbeat {
			fmt.Println("sending appendEntries heartbeat rpc")
		} else {
			fmt.Printf("sending appendEntries log rpc from %d to %d for entries ", rf.me, index)
			rf.PrintLog(args.Entries)
			fmt.Print(" \n")
		}
	*/
	ok := rf.peers[index].Call("Raft.AppendEntries", args, reply)
	return ok

}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (4B).
	rf.mu.Lock()

	isLeader = (rf.state == leader)

	if rf.state != leader {
		rf.mu.Unlock()
		return 0, 0, false
	}

	term = rf.currentTerm
	NewCommand := Entry{command, term}
	rf.log = append(rf.log, &NewCommand)

	rf.lastLogIndex = len(rf.log) - 1
	index = rf.lastLogIndex

	log.Printf("%d recieved new entry %v from client \n", rf.me, NewCommand)

	//fmt.Printf("%d current log state: ", rf.me)
	//rf.PrintLog(rf.log)
	//fmt.Print("\n")

	rf.persist()

	rf.mu.Unlock()

	rf.Leader()

	return index, term, isLeader
}

func (rf *Raft) PrintLog(log []*Entry) {
	/*
		for entry := range log {
			fmt.Print(log[entry])
		}
	*/

}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// (4A)

		time.Sleep(time.Duration(10) * time.Millisecond)

		rf.mu.Lock()

		if rf.state != leader {

			if time.Now().After(rf.electionTimeout) {

				rf.candVotes = 0

				rf.votedFor = rf.me
				rf.state = candidate
				rf.currentTerm++
				rf.candVotes++

				rf.persist()

				rf.resetTimeout()

				rf.mu.Unlock()

				go rf.getVotes()

			} else {
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) resetTimeout() {
	new := time.Duration(300 + rand.Intn(200))
	rf.electionTimeout = time.Now().Add(new * time.Millisecond)
}

func (rf *Raft) getVotes() {

	for index := range rf.peers {

		if index == rf.me {
			continue
		}

		rf.mu.Lock()

		var lastIndex int
		var lastTerm int

		if len(rf.log) == 0 {
			lastIndex = -1
			lastTerm = -1
		} else {
			lastIndex = len(rf.log) - 1
			lastTerm = rf.log[len(rf.log)-1].TermSeen
		}

		args := RequestVoteArgs{
			CandTerm:     rf.currentTerm,
			CandID:       rf.me,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		}
		rf.mu.Unlock()

		go func(index int) {

			reply := RequestVoteReply{}

			log.Printf("%d sending requestVote rpc to %d \n", rf.me, index)

			ok := rf.peers[index].Call("Raft.RequestVote", &args, &reply)

			if !ok {
				return
			} else {

				rf.mu.Lock()

				if rf.BecameFollower(reply.Term) {
					//fmt.Printf("%d became follower to %d from sending requestVote \n", rf.me, index)
					rf.mu.Unlock()
					return
				} else {
					rf.mu.Unlock()
				}

				if reply.Term != args.CandTerm {
					return
				}

				if reply.VoteGranted {
					rf.mu.Lock()

					if rf.state == candidate {
						rf.candVotes++
					}

					rf.mu.Unlock()
				}
			}

		}(index)

	}

	rf.BecomeLeader(true)
}

func (rf *Raft) BecomeLeader(countingVotes bool) {

	rf.mu.Lock()
	votingTerm := rf.currentTerm
	rf.mu.Unlock()

	for countingVotes {
		rf.mu.Lock()

		//fmt.Printf("%d has %d votes \n", rf.me, rf.candVotes)

		majority := (rf.candVotes > (len(rf.peers) / 2))

		//fmt.Printf("%d has %d votes and majority: %t \n", rf.me, rf.candVotes, majority)

		if majority && rf.state == candidate {
			log.Printf("%d becoming leader \n", rf.me)
			rf.state = leader
			//rf.matchIndex = 0
			//fmt.Printf("%d became leader \n", rf.me)

			rf.resetTimeout()

			rf.candVotes = 0

			rf.persist()

			//fmt.Printf("%d became leader \n", rf.me)

			rf.mu.Unlock()

			go rf.Heartbeats()

			//rf.Leader()

			countingVotes = false

		} else if rf.state == candidate {
			rf.mu.Unlock()
			time.Sleep(time.Duration(10) * time.Millisecond)
		} else if rf.state != candidate || votingTerm != rf.currentTerm {

			rf.mu.Unlock()

			countingVotes = false
		}
	}

}

func (rf *Raft) Leader() {

	rf.mu.Lock()

	me := rf.me
	peers := rf.peers
	//term := rf.currentTerm
	//commitIndex := rf.commitIndex
	//nextIndex := rf.nextIndex
	//log := rf.log
	//matchIndex := rf.matchIndex
	//lastLogIndex := rf.lastLogIndex
	//matchIndex[me] = lastLogIndex
	//nextIndex[me] = lastLogIndex + 1

	rf.mu.Unlock()

	rf.CheckCommit()

	for peer := range peers {

		if peer == me {
			continue
		}

		args := AppendEntriesArgs{}
		reply := AppendEntriesReply{}

		rf.mu.Lock()

		args.LeadTerm = rf.currentTerm
		args.Heartbeat = false
		args.LeadId = rf.me
		args.LeadCommit = rf.commitIndex
		prevLogIndex := rf.nextIndex[peer] - 1
		args.PrevLogIndex = prevLogIndex
		if prevLogIndex >= 0 {
			args.PrevLogTerm = rf.log[prevLogIndex].TermSeen
		} else {
			args.PrevLogTerm = -1
		}

		if rf.nextIndex[peer] <= rf.lastLogIndex {
			args.Entries = rf.log[prevLogIndex+1 : rf.lastLogIndex+1]
		}

		rf.mu.Unlock()

		rf.mu.Lock()

		log.Printf("%d sending entries ", rf.me)
		rf.PrintLog(args.Entries)
		log.Printf(" to %d \n", peer)

		rf.mu.Unlock()

		if len(args.Entries) != 0 {

			go func(peer int) {

				rf.sendAppendEntries(peer, &args, &reply)

				rf.mu.Lock()

				if reply.Success {
					rf.matchIndex[peer] = prevLogIndex + len(args.Entries)
					rf.nextIndex[peer] = min(rf.nextIndex[peer]+len(args.Entries), rf.lastLogIndex+1)
				} else {
					if reply.NewTerm > args.LeadTerm {
						rf.BecameFollower(reply.NewTerm)
						//fmt.Printf("%d became follower to %d from sending appendEntries \n", rf.me, peer)
						rf.mu.Unlock()
						return
					}

					if reply.LatestTerm == -1 {
						rf.nextIndex[peer] = reply.LastestLength
						rf.mu.Unlock()
						return
					}

					index := -1

					for i, j := range rf.log {
						if j.TermSeen == reply.LatestTerm {
							index = i
						}
					}

					if index == -1 {
						rf.nextIndex[peer] = reply.LatestIndex
					} else {
						rf.nextIndex[peer] = index
					}

				}

				rf.mu.Unlock()

			}(peer)
		}

		//rf.CheckCommit()

	}

	//rf.mu.Lock()
	//fmt.Printf("Leader %d current log state: ", rf.me)
	//rf.PrintLog(rf.log)
	//fmt.Printf("\n")
	//fmt.Printf("Leader %d current commit index: %d \n", rf.me, rf.commitIndex)
	//rf.mu.Unlock()
}

func (rf *Raft) CheckCommit() {
	rf.mu.Lock()

	me := rf.me
	peers := rf.peers
	term := rf.currentTerm
	commitIndex := rf.commitIndex
	nextIndex := rf.nextIndex
	log := rf.log
	matchIndex := rf.matchIndex
	lastLogIndex := rf.lastLogIndex
	matchIndex[me] = lastLogIndex
	nextIndex[me] = lastLogIndex + 1

	rf.mu.Unlock()

	for i := commitIndex + 1; i <= lastLogIndex; i++ {
		count := 0
		total := len(peers)
		majority := (total / 2) + 1

		rf.mu.Lock()
		for peer := range peers {
			if matchIndex[peer] >= i && log[i].TermSeen == term {
				count++
			}
		}

		rf.mu.Unlock()

		if count >= majority {
			rf.mu.Lock()

			for j := rf.commitIndex + 1; j <= i; j++ {

				toApply := ApplyMsg{
					CommandValid: true,
					Command:      log[j].Action,
					CommandIndex: j,
				}

				rf.applyMsg <- toApply

				//fmt.Printf("%d committing entry %v at index %d \n", rf.me, log[j].Action, j)

				rf.commitIndex++

				//go rf.Leader()
			}

			rf.mu.Unlock()

		}
	}
}

func (rf *Raft) BecameFollower(leadTerm int) bool {
	changeState := false
	if leadTerm > rf.currentTerm {
		rf.resetTimeout()
		rf.currentTerm = leadTerm
		rf.state = follower
		rf.votedFor = -1
		changeState = true
		rf.candVotes = 0
		rf.persist()
		/*
			rf.persist()
			fmt.Printf("%d became follower with log", rf.me)
			rf.PrintLog(rf.log)
			fmt.Printf("\n")
		*/
	}
	return changeState
}

func (rf *Raft) Heartbeats() {
	for !rf.killed() {

		//rf.Leader()

		rf.mu.Lock()

		if rf.state != leader {
			rf.mu.Unlock()
			continue
		}

		rf.mu.Unlock()

		rf.CheckCommit()

		for peer := range rf.peers {

			rf.mu.Lock()

			if peer == rf.me {
				rf.mu.Unlock()
				continue
			}

			args := AppendEntriesArgs{
				Heartbeat:    true,
				LeadTerm:     rf.currentTerm,
				LeadCommit:   rf.commitIndex,
				LeadId:       rf.me,
				PrevLogIndex: rf.nextIndex[rf.me] - 1,
			}

			if rf.nextIndex[peer]-1 >= 0 {
				args.PrevLogTerm = rf.log[rf.nextIndex[peer]-1].TermSeen
			} else {
				args.PrevLogTerm = -1
			}

			//fmt.Printf("%d sending heartbeats for term %d \n", rf.me, rf.currentTerm)

			go func(peer int, args AppendEntriesArgs) {

				reply := AppendEntriesReply{}

				rf.mu.Unlock()

				rf.sendAppendEntries(peer, &args, &reply)

				if !reply.Success {
					rf.mu.Lock()

					rf.BecameFollower(reply.NewTerm)

					rf.mu.Unlock()
				}

			}(peer, args)
		}

		time.Sleep(time.Duration(rf.heartbeats) * time.Millisecond)

	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
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

	// Your initialization code here (4A, 4B).

	rf.mu = sync.Mutex{}
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	//rf.matchIndex = 0
	rf.state = follower
	rf.votedFor = -1
	rf.candVotes = 0
	rf.heartbeats = 150

	rf.applyMsg = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.log = []*Entry{
		{
			Action:   nil,
			TermSeen: 0,
		},
	}

	rand.Seed(time.Now().Local().UnixNano())

	rf.mu.Lock()

	rf.resetTimeout()

	rf.mu.Unlock()

	// initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections.
	go rf.ticker()

	return rf
}
