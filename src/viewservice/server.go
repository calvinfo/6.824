package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
//import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
  times map[string] time.Time
  views map[uint] View
  currentView uint
  primaryAck uint
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()

  vs.times[args.Me] = time.Now()

  //log.Printf("[View Service] Received ping(%d): %s", args.Viewnum, args.Me)

  view := vs.views[vs.currentView]

  if view.Primary == args.Me && view.Viewnum == args.Viewnum {
    //log.Printf("[View Service] Primary Ack: %d", view.Viewnum);
    vs.primaryAck = view.Viewnum
  }

  newView := UpdateView(&view, args.Me, args.Viewnum)

  if newView.Viewnum != vs.currentView && newView.Viewnum <= vs.primaryAck + 1 {

    /*log.Printf(`[View Service] Updating View(%d):
                               Primary: (%s)
                               Backup: (%s)`, newView.Viewnum, newView.Primary,
                                              newView.Backup)*/

    vs.currentView = newView.Viewnum
    vs.views[newView.Viewnum] = newView
    reply.View = newView
  } else {
    // Reply with the current view
    reply.View = view
  }

  return nil
}


//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()

  reply.View = vs.views[vs.currentView]

  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
  vs.mu.Lock()
  defer vs.mu.Unlock()

  view := vs.views[vs.currentView]

  primaryTime, pExists := vs.times[view.Primary]
  backupTime, bExists := vs.times[view.Backup]

  // If our primary is behind the current view, don't remove anyone.
  if (vs.primaryAck < vs.currentView) {
    return
  }

  // If the primary has elapsed, try and replace it
  if pExists && time.Since(primaryTime) > PingInterval * DeadPings {

    /*log.Printf(`[View Service] Detected Primary Dead(%d):
                               Primary: (%s)
                               Backup: (%s)`, view.Viewnum, view.Primary,
                                              view.Backup)*/

    newView := View{ Viewnum: view.Viewnum + 1,
                     Primary: view.Backup }

    newView.Backup = findBackup(vs.times, &view)

    vs.currentView = newView.Viewnum
    vs.views[vs.currentView] = newView

    /*log.Printf(`[View Service] Updating View, Primary Dead(%d):
                               Primary: (%s)
                               Backup: (%s)`, newView.Viewnum, newView.Primary,
                                              newView.Backup)*/
  } else if bExists && time.Since(backupTime) > PingInterval * DeadPings {

    // Only replace the secondary.
    newView := View{ Viewnum: view.Viewnum + 1,
                     Primary: view.Primary }

    newView.Backup = findBackup(vs.times, &view)

    vs.currentView = newView.Viewnum
    vs.views[vs.currentView] = newView
  }
}


//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.times = make(map[string]time.Time)
  vs.views = make(map[uint]View)
  vs.currentView = 0
  vs.views[vs.currentView] = View{}

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        //fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}

func UpdateView(view *View, server string, clientnum uint) View {

  var result View = View{ Viewnum: view.Viewnum,
                          Primary: view.Primary,
                          Backup:  view.Backup }

  var changed bool = false

  if clientnum == 0 && // client thinks it's a backup
     view.Viewnum > 1 && // view number is over one
     server == view.Primary {

    //log.Printf("[View Service] Primary Restarted: %s", server)
    view.Primary = ""
  }

  if len(view.Primary) == 0 {

    if len(view.Backup) > 0 {
      result.Primary = view.Backup
      result.Backup  = server
    } else {
      result.Primary = server
    }
    changed = true
  } else {
    if len(view.Backup) == 0 && view.Primary != server {
      result.Backup = server
      changed = true
    }
  }

  if changed {
    result.Viewnum = view.Viewnum + 1
  }

  return result
}



func findBackup(times map[string]time.Time, view *View) string {

  for key, value := range times {
    if time.Since(value) < PingInterval * DeadPings &&
       key != view.Primary && key != view.Backup {
      return key
    }
  }

  return ""
}