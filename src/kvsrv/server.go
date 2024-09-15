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
	executed map[string]map[string]string
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
	kv.executed[args.Key] = make(map[string]string)
	reply.Value = val
}

// DUPLICATE REQUESTS ARE ALL ON THE SAME KEY
// SO THE EVICITION POLICY SHOULD BE BASED ON THE KEY
// MAKE SOMETHING LIKE LRU
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	operationIds := kv.executed[args.Key]
	if _, ok := operationIds[args.OperationID]; ok {
		return
	}
	kv.cache[args.Key] = args.Value
	operationIds, ok := kv.executed[args.Key]
	if !ok {
		operationIds = make(map[string]string)
		kv.executed[args.Key] = operationIds
	}
	operationIds[args.OperationID] = args.Value
	kv.executed[args.Key] = operationIds

	if len(operationIds) > 100 {
		kv.cleanup(args.Key)
	}

	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	operationIds := kv.executed[args.Key]

	if storedVal, ok := operationIds[args.OperationID]; ok {
		reply.Value = storedVal
		return
	}

	val, ok := kv.cache[args.Key]
	if !ok {
		val = ""
	}

	appendedVal := val + args.Value

	kv.cache[args.Key] = appendedVal
	operationIds, ok = kv.executed[args.Key]
	if !ok {
		operationIds = make(map[string]string)
		kv.executed[args.Key] = operationIds
	}
	operationIds[args.OperationID] = val
	kv.executed[args.Key] = operationIds
	if len(operationIds) > 100 {
		kv.executed[args.Key] = make(map[string]string)

	}
	reply.Value = val
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.

	kv.cache = map[string]string{}
	kv.executed = map[string]map[string]string{}
	return kv
}

func (kv *KVServer) cleanup(keyToClean string) {
	kv.executed[keyToClean] = map[string]string{}
}
