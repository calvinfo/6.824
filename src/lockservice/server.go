package lockservice

import "net"
import "net/rpc"
import "log"
import "sync"
import "fmt"
import "os"
import "io"
import "time"

type LockRecord struct {

  client int
  request int
  result bool
}


type LockServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool  // for test_test.go
  dying bool // for test_test.go

  am_primary bool // am I the primary?
  backup string   // backup's port

  // for each lock name, is it locked?
  locks map[string]bool

  // for each client keep a map of requests
  requests map[int]LockRecord
}




//
// server Lock RPC handler.
//
// you will have to modify this function
//
func (ls *LockServer) Lock(args *LockArgs, reply *LockReply) error {
  ls.mu.Lock()
  defer ls.mu.Unlock()


  locked, _ := ls.locks[args.Lockname]
  record, present := ls.requests[args.ClientId]

  if ls.am_primary {
    var backupReply LockReply
    call(ls.backup, "LockServer.Lock", args, &backupReply)
  }


  if present == true &&
    record.request == args.RequestId {

    reply.OK = record.result
  } else {

    if locked {
      reply.OK = false
    } else {
      reply.OK = true
      ls.locks[args.Lockname] = true
    }
  }

  host := ""
  if ls.am_primary { host = "primary" } else { host = "secondary" }

  log.Printf("[%s][%d][%d] Lock %s: %t", host,
            args.ClientId, args.RequestId, args.Lockname, reply.OK)


  record = LockRecord{ client  : args.ClientId,
                       request : args.RequestId,
                       result  : reply.OK }

  ls.requests[args.ClientId] = record

  return nil
}

//
// server Unlock RPC handler.
//
func (ls *LockServer) Unlock(args *UnlockArgs, reply *UnlockReply) error {
  ls.mu.Lock()
  defer ls.mu.Unlock()

  locked, _ := ls.locks[args.Lockname]
  record, present := ls.requests[args.ClientId]

  if ls.am_primary {
    var backupReply LockReply
    call(ls.backup, "LockServer.Unlock", args, &backupReply)
  }

  if present == true &&
     record.request == args.RequestId {

    reply.OK = record.result

  } else {

    if locked {
      reply.OK = true
      ls.locks[args.Lockname] = false
    } else {
      reply.OK = false
    }
  }

  host := ""
  if ls.am_primary { host = "primary" } else { host = "secondary" }

  log.Printf("[%s][%d][%d] Unlock %s: %t", host,
            args.ClientId, args.RequestId, args.Lockname, reply.OK)

  record = LockRecord{ client : args.ClientId, request : args.RequestId, result : reply.OK }
  ls.requests[args.ClientId] = record

  return nil
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this.
//
func (ls *LockServer) kill() {
  ls.dead = true
  ls.l.Close()
}

//
// hack to allow test_test.go to have primary process
// an RPC but not send a reply. can't use the shutdown()
// trick b/c that causes client to immediately get an
// error and send to backup before primary does.
// please don't change anything to do with DeafConn.
//
type DeafConn struct {
  c io.ReadWriteCloser
}
func (dc DeafConn) Write(p []byte) (n int, err error) {
  return len(p), nil
}
func (dc DeafConn) Close() error {
  return dc.c.Close()
}
func (dc DeafConn) Read(p []byte) (n int, err error) {
  return dc.c.Read(p)
}

func StartServer(primary string, backup string, am_primary bool) *LockServer {
  ls := new(LockServer)
  ls.backup = backup
  ls.am_primary = am_primary
  ls.locks = map[string]bool{}
  ls.requests = make(map[int]LockRecord)

  // Your initialization code here.


  me := ""
  if am_primary {
    me = primary
  } else {
    me = backup
  }

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(ls)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(me) // only needed for "unix"
  l, e := net.Listen("unix", me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  ls.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for ls.dead == false {
      conn, err := ls.l.Accept()
      if err == nil && ls.dead == false {
        if ls.dying {
          // process the request but force discard of reply.

          // without this the connection is never closed,
          // b/c ServeConn() is waiting for more requests.
          // test_test.go depends on this two seconds.
          go func() {
            time.Sleep(2 * time.Second)
            conn.Close()
          }()
          ls.l.Close()

          // this object has the type ServeConn expects,
          // but discards writes (i.e. discards the RPC reply).
          deaf_conn := DeafConn{c : conn}

          rpcs.ServeConn(deaf_conn)

          ls.dead = true
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && ls.dead == false {
        fmt.Printf("LockServer(%v) accept: %v\n", me, err.Error())
        ls.kill()
      }
    }
  }()

  return ls
}
