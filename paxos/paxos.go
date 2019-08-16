package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
)

type ErrCode int
type State int

const Debug = 1 //0:close 1:open

func lPrint(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	ErrOK ErrCode = ErrCode(iota)
	ErrNetwork
	ErrRejected
)

const (
	StateUndecided State = State(iota)
	StateDecided
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instance map[int]*PaxosInst
	done     []int
}

// prepare阶段发送的消息
type PrepareReq struct {
	Seq   int
	Stamp int64
}

type PrepareRsp struct {
	value interface{}
	Stamp int64
	Code  ErrCode
}

type CommitReq struct {
	Seq   int
	value interface{}
	Stamp int64
}

type CommitRsp struct {
	Code ErrCode
}

type DecideReq struct {
	Seq   int
	value interface{}
	Svrid int
	Done  int
}

type DecideRsp struct {
}

type PaxosInst struct {
	status  State
	value   interface{}
	platest int64 //proposer
	alatest int64 //acceptor
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) AcceptDecide(req *DecideReq, rsp *DecideRsp) {
	px.mu.Lock()
	defer px.mu.Unlock()
	inst, ok := px.instance[req.Seq]
	if !ok {
		inst = &PaxosInst{StateDecided, req.value, 0, 0}
		px.instance[req.Seq] = inst
	} else {
		inst.status = StateDecided
		inst.value = req.value
	}

	px.done[req.Svrid] = req.Done

}

func (px *Paxos) SendDecide(seq int, v interface{}) {
	for i, p := range px.peers {
		if i == px.me {
			rsp := DecideRsp{}
			px.AcceptDecide(&DecideReq{seq, v, px.me, px.done[px.me]}, &rsp)
		} else {
			rsp := DecideRsp{}
			call(p, "Paxos.AcceotDecide", &DecideReq{seq, v, px.me, px.done[px.me]}, &rsp)
		}
	}
}

func (px *Paxos) AcceptCommit(req *CommitReq, rsp *CommitRsp) {
	inst, ok := px.instance[req.Seq]
	if !ok {
		inst = &PaxosInst{StateUndecided, req.value, req.Stamp, 0}
		px.instance[req.Seq] = inst
		rsp.Code = ErrOK
	} else if req.Stamp >= inst.platest {
		inst.platest = req.Stamp
		inst.value = req.value
		inst.alatest = req.Stamp
		rsp.Code = ErrOK
	} else {
		rsp.Code = ErrRejected
	}
}

func (px *Paxos) sendCommit(seq int, v interface{}, timestamp int64) bool {
	ch := make(chan CommitRsp, len(px.peers))
	for i, p := range px.peers {
		if i == px.me {
			go func() {
				rsp := CommitRsp{}
				px.AcceptCommit(&CommitReq{seq, v, timestamp}, &rsp)
				ch <- rsp
			}()
		} else {
			go func(host string) {
				rsp := CommitRsp{}
				px.AcceptCommit(&CommitReq{seq, v, timestamp}, &rsp)
				if ok := call(host, "Paxos.AcceptCommit", &CommitReq{seq, v, timestamp}, &rsp); !ok {
					rsp.Code = ErrNetwork
				}
				ch <- rsp
			}(p)
		}
	}

	Accept := 0
	for c := range ch {
		if c.Code == ErrOK {
			Accept++
		}
	}

	if Accept > len(px.peers) {
		return true
	}
	return false
}

func (px *Paxos) AcceptPrepare(req *PrepareReq, rsp *PrepareRsp) error {
	lPrint("Peers %d is handle Prepare request", px.me)
	px.mu.Lock()
	defer px.mu.Unlock()
	inst, ok := px.instance[req.Seq]
	if !ok {
		inst = &PaxosInst{StateUndecided, nil, 0, 0}
		px.instance[req.Seq] = inst
		rsp.Code = ErrOK
	} else if req.Stamp >= inst.platest {
		px.instance[req.Seq].platest = req.Stamp
		rsp.Code = ErrOK
	} else {
		rsp.Code = ErrRejected
	}
	rsp.value = inst.value
	rsp.Stamp = inst.alatest
	return nil

}

func (px *Paxos) sendPrepare(seq int, v interface{}) (bool, interface{}, int64) {
	lPrint("Peers %d is sending Prepare request", px.me)
	ch := make(chan PrepareRsp, len(px.peers))
	timestamp := time.Now().UnixNano()
	for i, p := range px.peers {
		if i == px.me {
			go func() {
				rsp := PrepareRsp{}
				px.AcceptPrepare(&PrepareReq{seq, timestamp}, &rsp)
				ch <- rsp
			}()
		} else {
			go func(host string) {
				rsp := PrepareRsp{}
				if ok := call(host, "Paxos.AcceptPrepare", &PrepareReq{seq, timestamp}, &rsp); !ok {
					rsp.Code = ErrNetwork
				}
				ch <- rsp
			}(p)
		}
	}
	Agreed := 0
	var nv interface{} = interface{}(nil)
	hightest := int64(0)
	for c := range ch {
		if c.Code == ErrOK {
			Agreed++
			if c.Stamp > hightest {
				nv = c.value
				hightest = c.Stamp
			}
		}
	}
	if Agreed > len(px.peers)/2 {
		return true, nv, timestamp
	}
	return false, nv, timestamp

}

// acceptor's prepare(n) handler:

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	lPrint("px.Min is %d", px.Min())
	if seq < px.Min() {
		return
	}
	fmt.Println("sdfffffff")
	go func() {
		lPrint("Peers %d is start", px.me)
		for {
			ok, nv, t := px.sendPrepare(seq, v)
			if !ok {
				continue
			} else if nv != nil {
				v = nv
			}
			if ok := px.sendCommit(seq, v, t); !ok {
				continue
			}
			px.SendDecide(seq, v)
			break
		}
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	if seq >= px.done[px.me] {
		px.done[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	highest := 0

	for key := range px.instance {
		if key > highest {
			highest = key
		}
	}
	return highest
	return 0
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	px.mu.Unlock()
	ans := 0x7fffffff

	for i := range px.done {
		lPrint("px.done[%d] is %d", i, px.done[i])
		//lPrint("%d", i)
		if ans > i {
			ans = px.done[i]
		}
	}
	if len(px.done) == 0 {
		lPrint("here")
		ans = 0
	}
	lPrint("ans is %d", ans)
	for seq, inst := range px.instance {
		if seq < ans && inst.status == StateDecided {
			delete(px.instance, seq)
		}
	}
	return ans + 1
	return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
// 应用程序想知道这个节点是否认为已经决定了一个实例，如果已经决定了，那么商定的值是多少。Status()应该只检查本地节点的状态;它不应该与其他Paxos节点联系。
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.
	inst, ok := px.instance[seq]
	if ok {
		return true, inst.value
	}
	return false, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instance = make(map[int]*PaxosInst)
	px.done = make([]int, len(px.peers))
	for i := 0; i < len(peers); i++ {
		px.done[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		//准备接收来自客户的连接。
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.颠覆

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						// 丢弃该请求
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						// 处理请求，但强制放弃应答。
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
