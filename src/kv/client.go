package kv

import "rpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*rpc.ClientEnd
	// TODO
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*rpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// TODO
	return ck
}

func (ck *Clerk) Get(key string) string {

	// TODO
	return ""
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// TODO
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
