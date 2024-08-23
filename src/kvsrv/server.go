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
	cache    map[string]string
	executed map[string]string
}

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

// DUPLICATE REQUESTS ARE ALL ON THE SAME KEY
// SO THE EVICITION POLICY SHOULD BE BASED ON THE KEY
// MAKE SOMETHING LIKE LRU
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.executed[args.OperationID]; ok {
		return
	}
	kv.cache[args.Key] = args.Value
	kv.executed[args.OperationID] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if storedVal, ok := kv.executed[args.OperationID]; ok {
		reply.Value = storedVal
		return
	}

	val, ok := kv.cache[args.Key]
	if !ok {
		val = ""
	}

	appendedVal := val + args.Value

	kv.cache[args.Key] = appendedVal
	kv.executed[args.OperationID] = val
	reply.Value = val
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.

	kv.cache = map[string]string{}
	kv.executed = map[string]string{}
	return kv
}
