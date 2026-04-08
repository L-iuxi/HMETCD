package raft

import (
	"TicketX/internal/rpc"
	"bytes"
	"math/rand"
	"sync"
	"time"
)

// 传输消息的结构体
type ApplyMsg struct {
	CommandIndex int
	Command      interface{} //不关心类型
	CommandValid bool
}

// 定义server状态
type State string

const (
	Leader    State = "Leader"
	Candidate State = "Candidate"
	Follower  State = "Follower"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft结构体
type Raft struct {
	mu        sync.Mutex
	me        int              //当前服务器在peer的下标
	peers     []*rpc.ClientEnd //存有所有服务器的组
	states    State            //状态
	term      int              //当前任期号
	vote      int              //投票给
	persister *rpc.Persister   // 持久化状态的对象，用来保存Raft状态，以便在崩溃和重启后恢复
	log       []LogEntry       //日志

	commitIndex      int           //等待提交的最新日志编号
	nextIndex        []int         //日志同步的位置（从哪里开始同步日志
	lastApply        int           //上次执行的最后一条日志编号
	applyCh          chan ApplyMsg //与kvserver沟通的渠道
	heartbeat        *time.Timer   //心跳超时
	overElectiontime *time.Timer   //选举超时
	lastSnapIndex    int           //上次截断日志的位置
	lastSnapTerm     int           //上次截断日志的任期
	snap             []byte
}

// 请求投票的结构体
type RequestVoteArgs struct {
	Term         int //当前任期号
	CandidateId  int //候选人id
	LastLogIndex int //候选人最新日志的index
	LastLogTerm  int //候选人最新日志的任期
}

// 投票给出的回复
type RequestVoteReply struct {
	Term   int //当前任期号
	IsVote int //是否投票
}

// 获取当前节点在当前任期是否leader
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.states == Leader {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.term
	return term, isleader
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := rpc.NewEncoder(w)

	e.Encode(rf.log)
	e.Encode(rf.vote)
	e.Encode(rf.term)
	e.Encode(rf.lastSnapIndex)
	e.Encode(rf.lastSnapTerm)

	return w.Bytes()

}

// 持久化保存当前raft状态，防止节点崩溃
func (rf *Raft) persist() {
	state := rf.encodeState()
	rf.persister.Save(state, rf.snap)
}

// 解码持久化信息
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := rpc.NewDecoder(r)

	var Log []LogEntry
	var Term int
	var Vote int
	var SnapIndex int
	var SnapTerm int

	if d.Decode(&Log) != nil || d.Decode(&Vote) != nil || d.Decode(&Term) != nil || d.Decode(&SnapIndex) != nil || d.Decode(&SnapTerm) != nil {
		//解码失败
	} else {
		rf.log = Log
		rf.vote = Vote
		rf.term = Term
		rf.lastSnapIndex = SnapIndex
		rf.lastSnapTerm = SnapTerm
	}

}

// how many bytes in Raft's persisted log?
// 读取raft日志中多少bytes
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// 获取
func (rf *Raft) getRealIndex(i int) int {
	return i - rf.lastSnapIndex
}

// 未快照截断的日志长度
func (rf *Raft) getLastIndex() int {
	return rf.lastSnapIndex + len(rf.log) - 1
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 接收投票请求，投出票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.IsVote = 0

	upToDate := false
	lastIndex := rf.getLastIndex()
	lastTerm := rf.log[lastIndex-rf.lastSnapIndex].Term

	if args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex) {
		upToDate = true
	}

	if args.Term > rf.term { //版本号大了，更新版本号，清空投票
		rf.term = args.Term
		rf.vote = -1
		rf.states = Follower
		rf.persist()
	}

	if args.Term < rf.term { //版本号小了，不投票
		reply.IsVote = 0
		reply.Term = rf.term
		return
	}

	if args.Term == rf.term &&
		(rf.vote == -1 || rf.vote == args.CandidateId) && upToDate { //版本号相同，未投票

		rf.vote = args.CandidateId
		reply.IsVote = 1
		rf.persist()
		rf.overElectiontime.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)
	}
	reply.Term = rf.term
}

func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.states == Leader { //只有leader可以操作log
		addlog := LogEntry{
			Command: command,
			Term:    rf.term,
		}
		rf.log = append(rf.log, addlog) //把传过来的命令加入日志

		index = len(rf.log) - 1 //log从1开始
		rf.commitIndex = index  //单机情况下，加入即提交，后面要修改

		return index, rf.term, true
	} else {
		return 0, 0, false
	}
}

// 无限循环选举，心跳发送
func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()
		state := rf.states
		rf.mu.Unlock()
		switch state {

		case Follower, Candidate: //follower和Candidate
			select {
			case <-rf.overElectiontime.C: //到达选举时间，给select一个信号使它触发

				rf.mu.Lock() //锁

				rf.states = Candidate
				rf.term++
				rf.vote = rf.me //为自己投一票

				term := rf.term
				me := rf.me

				rf.persist()

				// 重置选举超时(确保时间现在不是正在触发/刚触发状态，再重置选举超时时间)
				if !rf.overElectiontime.Stop() {
					select {
					case <-rf.overElectiontime.C:
					default:
					}
				}
				rf.overElectiontime.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)

				votes := 1 //该选举者获取的总票数
				lastIndex := rf.getLastIndex()
				lastterm := rf.log[lastIndex-rf.lastSnapIndex].Term
				rf.mu.Unlock()

				for i := range rf.peers {
					if i == me {
						continue
					}

					go func(server int) {

						args := &RequestVoteArgs{
							Term:         term,
							CandidateId:  me,
							LastLogIndex: lastIndex,
							LastLogTerm:  lastterm,
						}
						reply := &RequestVoteReply{}

						ok := rf.sendRequestVote(server, args, reply)
						if !ok {
							return
						}

						rf.mu.Lock()

						// 过滤旧term
						if rf.states != Candidate || rf.term != term {
							rf.mu.Unlock()
							return
						}

						//发现更高term
						if reply.Term > rf.term {
							rf.term = reply.Term
							rf.states = Follower
							rf.vote = -1
							rf.persist()
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
						if reply.IsVote == 1 {

							rf.mu.Lock()
							votes++

							if votes > len(rf.peers)/2 && rf.states == Candidate {

								rf.states = Leader
								rf.heartbeat.Reset(0)

								// 当选为leader之后立刻发一次心跳告诉所有人

								for j := range rf.peers {
									rf.nextIndex[j] = rf.getLastIndex() + 1 //立刻更新对齐数
								}
							}
							rf.mu.Unlock()
						}
					}(i)
				}
			default:
				time.Sleep(10 * time.Millisecond)
			}
		case Leader: //leader发送心跳

			select {
			case <-rf.heartbeat.C:
				rf.broadcastAppendEntries() //心跳
				rf.heartbeat.Reset(50 * time.Millisecond)
			default:
				time.Sleep(10 * time.Millisecond)
			}

		}
	}
}

// 检查是否有新日志可以提交，更新已执行的日志
func (rf *Raft) ApplyLoop() {
	for {
		rf.mu.Lock()
		if rf.lastApply < rf.commitIndex { //提交从commitindex到lastapply这个区间的日志
			rf.lastApply++

			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApply,
				Command:      rf.log[rf.lastApply].Command,
			}

			rf.mu.Unlock()
			rf.applyCh <- msg //将要执行的日志传给上层kvserver
		} else { //没有新日志要提交
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func MakeRaft(applyCh chan ApplyMsg, peers []*rpc.ClientEnd, me int, persister *rpc.Persister) *Raft {
	rf := &Raft{}
	rf.me = me       //暂时只有一个节点
	rf.peers = peers //暂时只有一个节点
	rf.term = 0
	rf.states = Follower
	rf.vote = -1
	rf.persister = persister
	rf.lastSnapIndex = 0
	rf.lastSnapTerm = 0
	rf.nextIndex = make([]int, 0)
	rf.commitIndex = 0   //刚开始没有待提交的日志
	rf.lastApply = 0     //刚开始没有已经执行的日志
	rf.applyCh = applyCh //与上层kvserver联系的管道
	rf.snap = persister.ReadSnapshot()
	rf.overElectiontime = time.NewTimer(time.Duration(150+rand.Intn(150)) * time.Millisecond) //随机生成选举超时
	rf.heartbeat = time.NewTimer(50 * time.Millisecond)                                       //固定心跳发送时间

	rf.log = []LogEntry{{}} //dummy节点，log的index从1开始

	go rf.ApplyLoop() //循环发送要执行的日志给kvserver
	go rf.ticker()
	return rf

}
