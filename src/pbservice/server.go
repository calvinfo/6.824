package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "os"
import "syscall"
import "math/rand"


type PBServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  // Your declarations here.
  view viewservice.View
  values map[string]string
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.view.Primary != pb.me {
    reply.Err = ErrWrongServer
    log.Printf("[PbService] Contacted wrong server: %s", pb.me)
    return nil
  }

  val, present := pb.values[args.Key]

  if !present {
    reply.Err = ErrNoKey
    log.Printf("[PbService] Missing Key %s", args.Key)
  } else {
    reply.Err = OK
    reply.Value = val
    log.Printf("[PbService] Get %s: %s", args.Key, val)
  }

  return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.view.Primary != pb.me {
    reply.Err = ErrWrongServer
    log.Printf("[PbService] Contacted wrong server: %s", pb.me)
    return nil
  }

  log.Printf("[PbService] Set %s: %s", args.Key, args.Value)
  pb.values[args.Key] = args.Value

  // Forward to the backup
  if pb.view.Backup != "" {
    forwardArgs := ForwardArgs{ Key : args.Key, Value : args.Value }
    forwardReply := ForwardReply{}
    call(pb.view.Backup, "PBServer.Forward", forwardArgs, &forwardReply)
  }

  reply.Err = OK
  return nil
}


func (pb *PBServer) Forward(args *ForwardArgs, reply *ForwardReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  log.Printf("[PbService] Forward %s: %s", args.Key, args.Value)

  pb.values[args.Key] = args.Value
  reply.Err = OK
  return nil
}


func (pb *PBServer) Backup(args *BackupArgs, reply *BackupReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  log.Printf("[PbService] Backup received: %d keys", len(args.Values))
  pb.values = args.Values
  reply.Err = OK
  return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
  pb.mu.Lock()
  defer pb.mu.Unlock()
  result, _ := pb.vs.Ping(pb.view.Viewnum);
  if result.Viewnum != pb.view.Viewnum {
    log.Printf("[PbService] Updated View(%d)", result.Viewnum)

    if result.Backup != "" && pb.me == result.Primary {
      args := BackupArgs{ Values : pb.values }
      reply := BackupReply{}
      call(result.Backup, "PBServer.Backup", args, &reply)
    }

    pb.view = result
  }
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  // Your pb.* initializations here.
  pb.view = viewservice.View{}
  pb.values = make(map[string]string)

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
  }()

  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
  }()

  return pb
}
