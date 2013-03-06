package pbservice

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type PutArgs struct {
  Key string
  Value string
}

type PutReply struct {
  Err Err
}

type GetArgs struct {
  Key string
}

type GetReply struct {
  Err Err
  Value string
}


// Your RPC definitions here.
type ForwardArgs struct {
  Key string
  Value string
}

type ForwardReply struct {
  Err Err
}


type BackupArgs struct {
  Values map[string]string
}

type BackupReply struct {
  Err Err
}
