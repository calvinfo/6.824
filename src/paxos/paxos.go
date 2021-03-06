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

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

type Agreement struct {
  Seq int
  Decided bool
  Value interface{}
}


type Accept struct {
  Seq int
  Value interface{}
}

const (
  OK = "OK"
)

type Err string

type PrepareArgs struct {
  Seq int
}

type PrepareReply struct {
  OK bool
  Err Err
  Accept Accept
}


type AcceptArgs struct {
  Seq int
  Value interface{}
}

type AcceptReply struct {
  OK bool
  Accept Accept
  Err Err
}

type DecideArgs struct {
  Seq int
  Value interface{}
}

type DecideReply struct {}


type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here.
  instances map[int]Agreement
  agreements map[int]Agreement
  maxPrepare int
  accept Accept
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
  return false
}



//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  go px.SendPrepare(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.

}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  return px.maxPrepare
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
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
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
  return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peers state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  agreement, present := px.instances[seq]

  if !present {
    return false, nil
  }

  return agreement.Decided, agreement.Value
}


func (px *Paxos) SendPrepare(seq int, value interface{}) {

  log.Printf("[%d] Sending prepare %d", px.me, seq)

  var args PrepareArgs
  replies := make([]*PrepareReply, len(px.peers))
  for i, peer := range px.peers {
    args = PrepareArgs{Seq: seq}
    replies[i] = &PrepareReply{}
    call(peer, "Paxos.Prepare", args, &replies[i])
  }

  numReplies := 0
  highestAccept := Accept{Value : value}

  for _, reply := range replies {
    if reply.OK {
      numReplies += 1
      if reply.Accept.Seq > highestAccept.Seq {
        highestAccept = reply.Accept
      }
    }
  }

  log.Printf("[%d] Received %d replies for seq %d", px.me, numReplies, seq)

  if numReplies < (len(px.peers) / 2) {
    return;
  }

  acceptArgs := AcceptArgs{Seq   : seq,
                           Value : highestAccept.Value}

  acceptedReplies := make([]*AcceptReply, len(px.peers))
  numAccepted := 0

  for i, peer := range px.peers {
    acceptedReplies[i] = &AcceptReply{}
    call(peer, "Paxos.Accept", acceptArgs, &acceptedReplies[i])
  }

  for _, reply := range acceptedReplies {
    if reply.OK {
      numAccepted += 1
    }
  }

  if numAccepted < len(px.peers) / 2 {
    return
  }

  log.Printf("[%d] Sending decide for %d, value %v", px.me, seq, highestAccept.Value)

  decideArgs := DecideArgs{Seq   : seq,
                           Value : highestAccept.Value}
  decideReply := DecideReply{}

  for _, peer := range px.peers {
    call(peer, "Paxos.Decide", decideArgs, &decideReply)
  }
}


func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {

  log.Printf("[%d] Received accept call", px.me)
  px.mu.Lock()
  defer px.mu.Unlock()

  if (args.Seq >= px.maxPrepare) {
    px.maxPrepare   = args.Seq
    px.accept.Seq   = args.Seq
    px.accept.Value = args.Value
    px.agreements[args.Seq] = Agreement{Seq: args.Seq,
                                        Decided: false,
                                        Value: args.Value}
    reply.OK = true
  } else {
    reply.OK = false
  }

  return nil
}


func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {

  px.mu.Lock()
  defer px.mu.Unlock()

  log.Printf("[%d] Received prepare call", px.me)

  if (args.Seq > px.maxPrepare) {
    px.maxPrepare = args.Seq
    reply.OK = true
    reply.Accept = px.accept
  } else {
    reply.OK = false
  }

  log.Printf("[%d] Max prepare: %d, Seq: %d, OK: %t", px.me, px.maxPrepare, args.Seq, reply.OK)

  return nil
}


func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
  px.instances[args.Seq] = Agreement{Seq: args.Seq,
                                     Value: args.Value,
                                     Decided: true }
  return nil
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
  px.instances = make(map[int]Agreement)
  px.agreements = make(map[int]Agreement)
  px.accept = Accept{}
  px.maxPrepare = -1

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
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
