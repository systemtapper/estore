package raft

import (
	"rpc"
)
import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// raft node state
const (
	LEADER = iota
	FOLLOWER
	CANDIDATE
)

// append entries request message type
const (
	LOG_REPLICATION = iota
	HEARTBEAT
	LEADER_NOTIFICATION
)

type Raft struct {
	/* data not for raft protocol itself */
	// protect data access
	mu sync.Mutex
	// serialize the client request, or we can use a queue
	reqMu sync.Mutex
	// all peer nodes
	peers []*rpc.ClientEnd
	// stable storage
	persister *Persister
	// node id
	me int
	// for follower heartbeat timeout
	last_heartbeat_time time.Time
	// cache leader id
	leader int
	// applyChan, we use this to tell the test
	// framework to know any message has been committed
	applyChan chan ApplyMsg

	/* persistent state data for all states(LEADER, FOLLOWER, CANDIDATE) */
	// current term
	currentTerm int
	// candidate id voted for, -1 means not voted, during each
	// term it can only voted for one candidate for avoiding
	// split brain problem
	votedFor int
	// logs
	log []LogEntry

	/* volatile state data for all states(LEADER, FOLLOWER, CANDIDATE) */
	// node status
	role int
	// the highest log index to be committed, usuaally master -> follower
	commitIndex int
	// highest log index has been applied to replicated state machine
	lastApplied int

	/* volatile state data only for LEADER */
	// we can compute the log entries to be sent to followers with
	// nextIndex, matchIndex
	// for each server, index of the next log entry to send to
	// that server(initialized to leader last log index+1)
	nextIndex []int
	// for each server, index of highest log entry known to be
	// replicated(initialized to 0)
	matchIndex []int
}

// log entry
type LogEntry struct {
	Cmd      interface{}
	Term     int
	LogIndex int
}

// args for leader election request of candidate
type RequestVoteArgs struct {
	Term         int // term of this candidate
	CandidateId  int // id of this candidate
	LastLogIndex int // index of last log entry of this candidate
	LastLogTerm  int // term of last log entry of this candidate
}

// args for leader election reply messages
type RequestVoteReply struct {
	Term        int  // currentTerm from requested node, for candidate to update
	VoteGranted bool // true means get voted by requested node
}

// AppendEntries Request
type AppendEntriesArgs struct {
	// message types
	// LOG_REPLICATION(0)      =>   normal log replication
	// HEARTBEAT(1)            =>   messages
	// LEADER_NOTIFICATION(2)  =>   leader election notification
	Type int
	// leader term
	Term int
	// leader id
	LeaderId int
	// index of log entry immediately preceding new ones
	// used to check if follower can accept the new entries
	// if not we need to fill entries before first
	PrevLogIndex int
	PrevLogTerm  int
	// log entries to be set to followers, empty for leader
	// election notification and heartbeat
	Entries []LogEntry
	// leader's commit index to specify which log index can be committed
	// usually increased after leader AppendEntries succeeded
	CommitIndex int
}

// AppendEntries Reply
type AppendEntriesReply struct {
	// currentTerm of the follower, for leader to update
	Term int
	// true if follower contains entry matching preLogIndex and preLogTerm
	Success bool
}

// ApplyMsg sent to RSM to apply log entries
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	term := rf.currentTerm
	isleader := (rf.role == LEADER)
	return term, isleader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

func (rf *Raft) printf(format string, v ...interface{}) {
	s := fmt.Sprintf("Node %v Term %v Leader %v State %v VotedFor %v CommitIndex %v LastApplied %v : ", rf.me, rf.currentTerm, rf.leader, rf.role, rf.votedFor, rf.commitIndex, rf.lastApplied)
	log.Printf(s+format, v...)
}

// true if the first log entry is ahead of the second log entry
func compareTermIndex(term1, index1, term2, index2 int) bool {
	return term1 > term2 || (term1 == term2 && index1 >= index2)
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// ignore
	if args.Term < rf.currentTerm {
		reply.VoteGranted, reply.Term = false, rf.currentTerm
		return
	}
	// turn into follower
	// NOTE: everytime turn into follower then see larger term, we reset votedFor
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.role, rf.votedFor = args.Term, FOLLOWER, -1
		rf.printf("RequestVote : receive larger term, turn into follower\n")
	}
	// get last log entry of current node
	lastLogTerm := rf.log[len(rf.log)-1].Term
	lastLogIndex := rf.log[len(rf.log)-1].LogIndex
	logOk := compareTermIndex(args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logOk {
		rf.role, rf.votedFor, rf.currentTerm = FOLLOWER, args.CandidateId, args.Term
		reply.VoteGranted = true
		rf.printf("RequestVote : candidate %v with req %v, voted\n", args.CandidateId, args)
		return
	}
	// rejected by stale log
	reply.VoteGranted, reply.Term = false, rf.currentTerm
	rf.printf("RequestVote : candidate %v rejected with logIndex %v logTerm %v, my logs are %v, i have voted for %v, in phase %v\n", args.CandidateId, args.LastLogIndex, args.LastLogTerm, rf.log, rf.votedFor, rf.role)
	return
}

// returns true if rpc says the RPC was delivered.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// send RequestVote to all, used by leaderElection
func (rf *Raft) sendRequestVoteAll(rvq RequestVoteArgs) []bool {
	var wg sync.WaitGroup
	var mu sync.Mutex
	r := make([]bool, len(rf.peers))
	for i, _ := range rf.peers {
		r[i] = false
	}

	for i, _ := range rf.peers {
		if i == rf.me {
			r[i] = true
			continue
		}
		wg.Add(1)
		rf.printf("LEADER ELECTION : send request vote to %v\n", i)
		go func(id int) {
			defer wg.Done()
			reply := &RequestVoteReply{VoteGranted: false}
			res := rf.sendRequestVote(id, rvq, reply)
			if res == false {
				rf.printf("LEADER ELECTION : send to node %v failed\n", id)
				return
			}
			if reply.VoteGranted == true {
				mu.Lock()
				r[id] = true
				mu.Unlock()
				rf.printf("LEADER ELECTION : node %v voted\n", id)
			} else {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm, rf.role, rf.votedFor = reply.Term, FOLLOWER, -1
				}
				rf.mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	return r
}

// leader election logic
func (rf *Raft) leaderElection() {
	rf.printf("enter leaderElection\n")
	defer rf.printf("exit leaderElection\n")
	// NOTE: increase my term and vote for myself
	// since rf.currentTerm may be changed in req goroutine
	// we should construct req for all gorotinue here.
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rvq := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].LogIndex,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	rvqR := rf.sendRequestVoteAll(rvq)
	voted := 0
	for i, _ := range rvqR {
		if rvqR[i] == true {
			voted += 1
		}
	}

	if voted <= (len(rf.peers) / 2) {
		rf.printf("LEADER ELECTION : failed with votes %v\n", voted)
		return
	}

	// NOTE: we issue requests to other nodes cuncurrently, there
	// might be some request see larger term and turn myself into
	// follower, in this case, we drop the votes of this round.
	// if not, there may be two leaders during the larger term
	// which cause split brain problem.
	rf.mu.Lock()
	if rf.role != CANDIDATE {
		rf.mu.Unlock()
		rf.printf("LEADER ELECTION : dropped due to see a larger term in RequestVote or response\n")
		return
	}
	rf.mu.Unlock()

	rf.mu.Lock()
	rf.role, rf.leader = LEADER, rf.me
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	nextIndex := rf.log[len(rf.log)-1].LogIndex + 1
	for i, _ := range rf.peers {
		rf.nextIndex[i] = nextIndex
		rf.matchIndex[i] = 0
	}
	rf.printf("LEADER ELECTION : become new leader %v %v %v\n", len(rf.peers), nextIndex, rf.nextIndex)

	term := rf.currentTerm
	//logEntry := LogEntry{Cmd: -1, Term: rf.currentTerm, LogIndex: rf.log[len(rf.log)-1].LogIndex + 1}
	rf.mu.Unlock()
	// issue LEADER_NOTIFICATION
	r := rf.sendAppendEntriesAll(LogEntry{}, term, LEADER_NOTIFICATION)
	for i, _ := range r {
		if r[i] == false {
			rf.printf("LEADER NOTIFICATION : send to %v failed\n", i)
		}
	}

	/*
		 // replicate a nop log to followers, then leader will commit all previous logs
		 // and make it's RSM is up-to-date, this make read op of client lineariable, and
		 // will not return stale data.
		 //
		 // this will cause test of agreement fail, so we do not add this logic for now.

		for {
			rf.mu.Lock()
			if rf.role != LEADER {
				rf.mu.Unlock()
				return
			}
		rf.mu.Unlock()
		r := rf.sendAppendEntriesAll(logEntry, term, LOG_REPLICATION)
		voted = 0
		for i, _ := range r {
			if r[i] == true {
				voted += 1
			}
		}
		if voted > (len(rf.peers) / 2) {
			rf.mu.Lock()
			rf.log = append(rf.log, logEntry)
			rf.persist()
			for k, _ := range rf.peers {
				if r[k] == true {
					rf.nextIndex[k] = logEntry.LogIndex + 1
					rf.matchIndex[k] = logEntry.LogIndex
				}
			}
			rf.mu.Unlock()
			rf.printf("Start : reach agreement on log %v term %v nextIndex %v\n", logEntry.LogIndex, logEntry.Term, rf.nextIndex)
			break
		}
		}
	*/
	return
}

// consistency check
func (rf *Raft) checkConsistency(args AppendEntriesArgs) bool {
	if len(rf.log) < args.PrevLogIndex+1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return false
	}
	return true
}

// cover old conflict entries and append new entries
func (rf *Raft) replicateEntries(args AppendEntriesArgs) {
	j, i := 0, args.PrevLogIndex+1
	for ; i < len(rf.log) && j < len(args.Entries); i++ {
		rf.log[i] = args.Entries[j]
		j++
	}
	rf.log = rf.log[:i]
	for j < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[j])
		j++
	}
	return
}

// AppendEntries
func (rf *Raft) AppendEntriesRequest(args AppendEntriesArgs, reply *AppendEntriesReply) {
	//rf.printf("enter AppendEntriesRequest\n")
	//defer rf.printf("exit AppendEntriesRequest\n")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.printf("AppendEntriesRequest : received with type %v entries %v commitIndex %v prevLogTerm %v prevLogIndex %v, my logs are %v\n", args.Type, args.Entries, args.CommitIndex, args.PrevLogTerm, args.PrevLogIndex, rf.log)
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		rf.printf("AppendEntriesRequest : receive request with stale term\n")
		reply.Success, reply.Term = false, rf.currentTerm
		return
	}
	// NOTE: everytime turn into follower then see larger term, we reset votedFor
	// turn into follower and reset votedFor if term > currentTerm
	if args.Term > rf.currentTerm {
		rf.printf("AppendEntriesRequest : receive request with larger term, turn into follower\n")
		rf.currentTerm, rf.role, rf.leader, rf.votedFor = args.Term, FOLLOWER, args.LeaderId, -1
		rf.persist()
	}

	// update some data first
	rf.leader, rf.role = args.LeaderId, FOLLOWER
	rf.last_heartbeat_time = time.Now()

	// check consistency
	if !rf.checkConsistency(args) {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// cover and append logs
	rf.replicateEntries(args)

	// persist data
	rf.persist()

	// update commitIndex to min(args.CommitIndex, last log index)
	rf.commitIndex = args.CommitIndex
	t := rf.log[len(rf.log)-1].LogIndex
	if args.CommitIndex > t {
		rf.commitIndex = t
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	rf.printf("AppendEntriesRequest : after consistency check, my logs are %v\n", rf.log)

	// apply log
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		applyMsg := ApplyMsg{
			Index:       rf.lastApplied,
			Command:     rf.log[rf.lastApplied].Cmd,
			UseSnapshot: false,
		}
		rf.printf("AppendEntriesRequest : apply log entry %v\n", rf.log[rf.lastApplied])
		rf.applyChan <- applyMsg
	}
	return
}

func (rf *Raft) sendAppendEntriesRequest(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntriesRequest", args, reply)
}

// consistency repair for follower id, used by sendAppendEntriesRequest
func (rf *Raft) doConsistencyRepair(args AppendEntriesArgs, logEntry LogEntry, id int, msgType int) {
	//rf.printf("enter doConsistencyRepair for node %v with logEntry %v msgType %v", id, logEntry, msgType)
	//defer rf.printf("exit doConsistencyRepair for node %v with logEntry %v msgType %v", id, logEntry, msgType)
	for {
		rf.mu.Lock()
		// NOTE: when state is changed for some other reason, break
		if rf.role != LEADER {
			rf.mu.Unlock()
			break
		}
		req := args
		req.Entries = []LogEntry{}
		req.PrevLogTerm = rf.log[0].Term
		req.PrevLogIndex = rf.log[0].LogIndex
		if rf.nextIndex[id] > 1 {
			rf.nextIndex[id] -= 1
			req.PrevLogTerm = rf.log[rf.nextIndex[id]-1].Term
			req.PrevLogIndex = rf.log[rf.nextIndex[id]-1].LogIndex
		}
		if msgType == LOG_REPLICATION {
			// for convinience, we just send all log entries every time
			if logEntry.LogIndex > rf.nextIndex[id] {
				req.Entries = rf.log[rf.nextIndex[id]:]
			}
			req.Entries = append(req.Entries, logEntry)
		} else {
			req.Entries = rf.log[rf.nextIndex[id]:]
		}
		rf.mu.Unlock()
		reply := &AppendEntriesReply{Success: false}
		res := rf.sendAppendEntriesRequest(id, req, reply)
		if res == false {
			rf.printf("CONSISTENCY REPAIR : failed with network problems\n")
		}
		if reply.Success == true {
			rf.printf("CONSISTENCY REPAIR : success for node %v\n", id)
			rf.mu.Lock()
			rf.nextIndex[id] = rf.log[len(rf.log)-1].LogIndex + 1
			rf.matchIndex[id] = rf.log[len(rf.log)-1].LogIndex
			rf.mu.Unlock()
			break
		} else {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm, rf.role, rf.leader, rf.votedFor = reply.Term, FOLLOWER, -1, -1
			}
			rf.mu.Unlock()
		}
	}
	return
}

// send AppendEntriesRequest to all followers, return status array
func (rf *Raft) sendAppendEntriesAll(logEntry LogEntry, term int, msgType int) []bool {
	//rf.printf("enter sendAppendEntriesAll with logEntry %v msgType %v term %v\n", logEntry, msgType, term)
	//defer rf.printf("exit sendAppendEntriesAll with logEntry %v msgType %v term %v\n", logEntry, msgType, term)
	rf.mu.Lock()
	r := make([]bool, len(rf.peers))
	for i, _ := range rf.peers {
		r[i] = false
	}
	// NOTE: construct request before req goroutine since
	// raft data might be modified by req goroutine, we should
	// ensure req is the same except PreLogIndex, PreLogTerm
	// and Entries
	args := AppendEntriesArgs{
		Type:        msgType,
		Term:        term,
		LeaderId:    rf.me,
		CommitIndex: rf.commitIndex,
		Entries:     []LogEntry{},
	}
	rf.mu.Unlock()
	var wg sync.WaitGroup
	rf.printf("sendAppendEntriesAll : my logs are %v, sending log %v, nextIndex %v\n", rf.log, logEntry, rf.nextIndex)
	for i, _ := range rf.peers {
		if i == rf.me {
			r[i] = true
			continue
		}
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rf.mu.Lock()
			args.Entries = []LogEntry{}
			if msgType == LOG_REPLICATION {
				if logEntry.LogIndex > rf.nextIndex[id] {
					args.Entries = rf.log[rf.nextIndex[id]:]
				}
				args.Entries = append(args.Entries, logEntry)
			} else {
				args.Entries = rf.log[rf.nextIndex[id]:]
			}
			args.PrevLogTerm = rf.log[rf.nextIndex[id]-1].Term
			args.PrevLogIndex = rf.log[rf.nextIndex[id]-1].LogIndex
			rf.mu.Unlock()
			reply := &AppendEntriesReply{Success: false}
			res := rf.sendAppendEntriesRequest(id, args, reply)
			// NOTE: network problems, we return directly, but not
			// retry indefinitely for testing requirement(return asap)
			if res == false {
				rf.printf("sendAppendEntriesAll : failed to send request to %v\n", id)
				return
			}
			if reply.Success == true {
				r[id] = true
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.printf("sendAppendEntriesAll : get larger term, turn into follower\n")
				rf.currentTerm, rf.role, rf.leader, rf.votedFor = reply.Term, FOLLOWER, -1, -1
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			// consistency check fail, we minus nextIndex and reissue request
			rf.doConsistencyRepair(args, logEntry, id, msgType)
		}(i)

	}
	wg.Wait()
	return r
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// TODO: how to solve the following contradiction
// 1. make Start async and return immediately to keep testing framework
// not been blocked due to call Start.
// 2. Start will retry forever for the same cmd on the same term and
// log index.
// 3. different Start requests must be serialized since the log index
// and log entry need to be serialized.
//
// one solution might be like this: we can do sendAppendEntriesAll
// in another goroutine, this can make several Start request go along concurrently
// (I think like pipeline), different log index request can go along concurrently
// but in this way, the testing framework will fail. Also we need change the commit
// logic, since the latter log index must be applied after all it's previous logs
// were applied.
//

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.printf("enter Start with Command %v\n", command)
	defer rf.printf("exit Start with Command %v\n", command)
	// NOTE: serialize client request
	rf.reqMu.Lock()
	defer rf.reqMu.Unlock()

	rf.mu.Lock()
	term, isLeader := rf.GetState()
	preLogIndex := rf.log[len(rf.log)-1].LogIndex
	index := preLogIndex + 1
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	rf.mu.Unlock()
	logEntry := LogEntry{command, term, index}

	// NOTE: we should retry forever here until success
	// or not leader anymore, we must ensure that there
	// is only the same cmd during the same term and
	// log index. or there will be the following bad
	// situation:
	// 1. leader 0 Start a cmd1 and node 1 persist the
	// entry, but node 2, 3, 4 not received because of
	// netwoking problems
	// 2. since we did not loop to retry, then other
	// client may call leader 0 Start, and submit cmd2
	// node 2, 3, 4 receive cmd2 log entry and persisted
	// but node 1 did not received this req
	// 3. leader 1 becomes new leader
	// 4. then the consistency check will pass but with
	// different cmd on the same term and log index.
	// 5. leader1 reach a new log agreement and commit
	// previous log with cmd1, but node 2, 3, 4 committed
	// with cmd2
	//
	// since the testing framework assume the Start is async
	// but if we do the Start work in a goroutine, and keep
	// the client request concurrently safe, we need a big
	// lock to protect log entry index allocation, and this
	// actually serialize the Start routine.
	//
	// I think the solution for this problem is using pipeline
	// but this will introduce other complexity like how to
	// handle the commitIndex and rf.log modification correctly
	// between different concurrent Start routines.

	//for {
	//	rf.mu.Lock()
	//	if rf.role != LEADER {
	//		rf.mu.Unlock()
	//		break
	//	}
	//	rf.mu.Unlock()

	r := rf.sendAppendEntriesAll(logEntry, term, LOG_REPLICATION)
	voted := 0
	for i, _ := range r {
		if r[i] == true {
			voted += 1
		}
	}
	// NOTE: different from leader election response, here
	// we do not care if turned into follower by reply, since
	// we can still store this log safely and then enter
	// follower state.
	if voted > (len(rf.peers) / 2) {
		rf.mu.Lock()
		rf.log = append(rf.log, logEntry)
		rf.persist()
		for k, _ := range rf.peers {
			if r[k] == true {
				rf.nextIndex[k] = logEntry.LogIndex + 1
				rf.matchIndex[k] = logEntry.LogIndex
			}
		}
		rf.mu.Unlock()
		rf.printf("Start : reach agreement on log %v term %v nextIndex %v\n", logEntry.LogIndex, logEntry.Term, rf.nextIndex)
		//break
	}
	//}

	// NOTE: update commit index and apply logs for leader here
	rf.mu.Lock()
Loop:
	for idx := rf.log[len(rf.log)-1].LogIndex; idx > rf.commitIndex; idx-- {
		if rf.log[idx].Term == rf.currentTerm {
			cnt := 0
			for i, _ := range rf.peers {
				if rf.matchIndex[i] >= idx {
					cnt += 1
					if cnt > len(rf.matchIndex)/2 {
						rf.commitIndex = idx
						break Loop
					}
				}
			}
		}
	}
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		applyMsg := ApplyMsg{
			Index:       rf.lastApplied,
			Command:     rf.log[rf.lastApplied].Cmd,
			UseSnapshot: false,
		}
		rf.applyChan <- applyMsg
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// NOTE: heartbeat timeout and the scale of leaderElection random timeout
// will influence the leader election. the election timeout shoud >> rpc
func (rf *Raft) doHeartbeat() {
	tm := time.NewTimer(time.Second)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		select {
		case <-tm.C:
			// leader heartbeat logic
			if rf.role == LEADER {
				rf.mu.Lock()
				term := rf.currentTerm
				rf.mu.Unlock()
				r := rf.sendAppendEntriesAll(LogEntry{}, term, HEARTBEAT)
				for i, _ := range r {
					if r[i] == false {
						rf.printf("HEARTBEAT : send to %v failed\n", i)
					}
				}
				tm.Reset(time.Second)
			}

			// follower heartbeat logic
			if rf.role == FOLLOWER {
				if rf.last_heartbeat_time.Add(time.Second * 2).Before(time.Now()) {
					rf.printf("HEARTBEAT : lost heartbeat from %v, turn into candidate\n", rf.leader)
					rf.mu.Lock()
					rf.role, rf.leader, rf.votedFor = CANDIDATE, -1, -1
					rf.mu.Unlock()
					// set a random timer for candidate
					t := time.Millisecond * time.Duration(150+r.Intn(150*2))
					tm.Reset(t)
				} else {
					tm.Reset(time.Second)
				}
			}

			// candidate logic
			if rf.role == CANDIDATE {
				t := time.Millisecond * time.Duration(150+r.Intn(150*2))
				rf.printf("LEADER ELECTION : sleep for some time %v and reissue request\n", t)
				begin := time.Now()
				rf.leaderElection()
				rf.printf("LEADER ELECTION : leader election total time: %v\n", time.Now().Sub(begin))
				tm.Reset(t)
			}
		}
	}
}

// for the whole cluster to start first or recovery
// not for each follower rejoin the cluster
func Make(peers []*rpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		leader:    -1,
		// fill a defaul log entry in index 0
		log:        []LogEntry{LogEntry{Cmd: -1, Term: -1, LogIndex: 0}},
		applyChan:  applyCh,
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}
	// recover persisted and volatile state data
	rf.readPersist(persister.ReadRaftState())
	for i, _ := range peers {
		rf.nextIndex[i] = rf.log[len(rf.log)-1].LogIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.commitIndex, rf.lastApplied = 0, 0
	rf.role, rf.votedFor = FOLLOWER, -1

	// do heartbeat in background
	go func() {
		rf.doHeartbeat()
	}()
	return rf
}
