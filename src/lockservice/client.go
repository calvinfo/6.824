package lockservice

import "math/rand"
import "log"

//
// the lockservice Clerk lives in the client
// and maintains a little state.
//
type Clerk struct {
  servers [2]string // primary port, backup port

  request int // request id
  id int // the id of the client.
}


func MakeClerk(primary string, backup string) *Clerk {
  ck := new(Clerk)
  ck.servers[0] = primary
  ck.servers[1] = backup

  // Assign client a random id
  ck.id = rand.Intn(10000);
  return ck
}


//
// ask the lock service for a lock.
// returns true if the lock service
// granted the lock, false otherwise.
//
// you will have to modify this function.
//
func (ck *Clerk) Lock(lockname string) bool {
  // prepare the arguments.
  args := &LockArgs{}
  args.Lockname = lockname
  args.ClientId = ck.id
  args.RequestId = ck.request

  var reply LockReply

  // send an RPC request, wait for the reply.
  ok := call(ck.servers[0], "LockServer.Lock", args, &reply)
  if ok == false {
    // contact the secondary
    ok := call(ck.servers[1], "LockServer.Lock", args, &reply);

    if ok == false {
      ck.request += 1
      return false
    }
  }

  log.Printf("[%d][%d] Lock Response %s: %t",
            args.ClientId, args.RequestId, args.Lockname, reply.OK)

  ck.request += 1
  return reply.OK
}


//
// ask the lock service to unlock a lock.
// returns true if the lock was previously held,
// false otherwise.
//

func (ck *Clerk) Unlock(lockname string) bool {
  // prepare the arguments.
  args := &UnlockArgs{}
  args.Lockname = lockname
  args.ClientId = ck.id
  args.RequestId = ck.request

  var reply UnlockReply

  // send an RPC request, wait for the reply.
  ok := call(ck.servers[0], "LockServer.Unlock", args, &reply)
  if ok == false {
    // contact the secondary
    ok := call(ck.servers[1], "LockServer.Unlock", args, &reply);

    if ok == false {
      ck.request += 1
      return false
    }
  }

  log.Printf("[%d][%d] Unlock Response %s: %t",
            args.ClientId, args.RequestId, args.Lockname, reply.OK)

  ck.request += 1
  return reply.OK
}
