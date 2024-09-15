package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	cache       map[string]string
	lastApplied map[string]Entry
}

type Entry struct {
	operationId int
	returnVal   string
}

//key -> operationId -> value

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.cache[args.Key]
	if !ok {
		return
	}
	reply.Value = val
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastAppliedEntry, ok := kv.lastApplied[args.ClientID]
	if ok && args.OperationID <= lastAppliedEntry.operationId {
		return
	}
	kv.cache[args.Key] = args.Value

	kv.lastApplied[args.ClientID] = Entry{operationId: args.OperationID}
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastAppliedEntry, ok := kv.lastApplied[args.ClientID]
	if ok && args.OperationID <= lastAppliedEntry.operationId {
		reply.Value = lastAppliedEntry.returnVal
		return
	}

	val, ok := kv.cache[args.Key]
	if !ok {
		val = ""
	}

	appendedVal := val + args.Value

	kv.cache[args.Key] = appendedVal
	kv.lastApplied[args.ClientID] = Entry{operationId: args.OperationID, returnVal: val}
	reply.Value = val
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.

	kv.cache = map[string]string{}
	kv.lastApplied = map[string]Entry{}

	return kv
}
