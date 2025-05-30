package reply

/*
 * redis reply
 * 实现一些常量的固定回复
 */

// PongReply is +PONG
type PongReply struct{}

// PONG 回复
var pongBytes = []byte("+PONG\r\n")

// ToBytes marshal redis.Reply
func (r *PongReply) ToBytes() []byte {
	return pongBytes
}

var thePongReply = new(PongReply)

func MakePongReply() *PongReply {
	return thePongReply
}

// OkReply is +OK
type OkReply struct{}

// OK 回复
var okBytes = []byte("+OK\r\n")

// ToBytes marshal redis.Reply
func (r *OkReply) ToBytes() []byte {
	return okBytes
}

var theOkReply = new(OkReply)

// MakeOkReply returns an ok reply
func MakeOkReply() *OkReply {
	return theOkReply
}

// 空字符串回复。长度为-1
var nullBulkBytes = []byte("$-1\r\n")

// NullBulkReply is empty string
type NullBulkReply struct{}

// ToBytes marshal redis.Reply
func (r *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

// MakeNullBulkReply creates a new NullBulkReply
func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

// 空数组回复
var emptyMultiBulkBytes = []byte("*0\r\n")

// EmptyMultiBulkReply is an empty list
type EmptyMultiBulkReply struct{}

// ToBytes marshal redis.Reply
func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

// NoReply respond nothing, for commands like subscribe
type NoReply struct{}

// 空回复
var noBytes = []byte("")

// ToBytes marshal redis.Reply
func (r *NoReply) ToBytes() []byte {
	return noBytes
}
