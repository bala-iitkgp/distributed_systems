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
	mp map[string]string
	lastId map[int64]int64 // clientId to last OrderId
	lastValue map[int64]string // clientId to last Value
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	
	v, ok := kv.mp[args.Key]
	if ok {
		reply.Value = v
	} else {
		v = ""
		reply.Value = ""
	}
	
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	alreadyDone := (kv.lastId[args.ClientId] == args.OrderId)
	if (!alreadyDone) {
		kv.mp[args.Key] = args.Value
		kv.lastId[args.ClientId] = args.OrderId
		// kv.lastValue = "done"
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	alreadyDone  := (kv.lastId[args.ClientId] == args.OrderId)
	if (alreadyDone) {
		reply.Value = kv.lastValue[args.ClientId]
	} else {
		v, ok := kv.mp[args.Key]
		if ok {
			kv.mp[args.Key] = v + args.Value
		} else {
			v = ""
			kv.mp[args.Key] = args.Value
		}
		reply.Value = v
		kv.lastId[args.ClientId] = args.OrderId
		kv.lastValue[args.ClientId] = v
	}
	
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mp = make(map[string]string)
	kv.lastId = make(map[int64]int64)
	kv.lastValue = make(map[int64]string)
	return kv
}
