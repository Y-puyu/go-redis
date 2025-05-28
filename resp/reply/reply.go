package reply

import (
	"bytes"
	"go-redis/interface/resp"
	"strconv"
)

var (
	// 空回复
	nullBulkReplyBytes = []byte("$-1")

	// CRLF is the line separator of redis serialization protocol
	// RESP 固定结尾
	CRLF = "\r\n"
)

/* ---- Bulk Reply ---- */

// BulkReply stores a binary-safe string
// 回复普通的字符串
//
//	字符串回复，以 $ 开头
type BulkReply struct {
	Arg []byte
}

// MakeBulkReply creates  BulkReply
func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

// ToBytes marshal redis.Reply
func (r *BulkReply) ToBytes() []byte {
	if len(r.Arg) == 0 {
		return nullBulkReplyBytes
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

/* ---- Multi Bulk Reply ---- */

// MultiBulkReply stores a list of string
// 回复多个字符串，转换成 RESP 协议的回复
// 数组回复，以 * 开头
type MultiBulkReply struct {
	Args [][]byte
}

// MakeMultiBulkReply creates MultiBulkReply
func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

// ToBytes marshal redis.Reply
// 将 [][]byte 转换成 RESP 协议的回复
func (r *MultiBulkReply) ToBytes() []byte {
	argLen := len(r.Args)

	// 多字节拼装，使用 bytes.Buffer 效率会高
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

/* ---- Status Reply ---- */

// StatusReply stores a simple status string
// 回复一个状态
// 正常的状态回复，以 + 开头
type StatusReply struct {
	Status string
}

// MakeStatusReply creates StatusReply
func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

// ToBytes marshal redis.Reply
func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

/* ---- Int Reply ---- */

// IntReply stores an int64 number
// 回复一个数字
// 数字以 : 开头
type IntReply struct {
	Code int64
}

// MakeIntReply creates int reply
func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

// ToBytes marshal redis.Reply
func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

/* ---- Error Reply ---- */

// ErrorReply is an error and redis.Reply
// 实现一个表示错误返回的接口。
// 缝合系统的 Error 接口，也缝合 Reply 接口
type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

// StandardErrReply represents handler error
// 回复一个标准的错误回复
// 错误回复以 - 开头
type StandardErrReply struct {
	Status string
}

// ToBytes marshal redis.Reply
func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}

// MakeErrReply creates StandardErrReply
func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

// IsErrorReply returns true if the given reply is error
// 判断是否是一个正常回复。就判断是否第一个字节是否为 - 即可。
func IsErrorReply(reply resp.Reply) bool {
	return reply.ToBytes()[0] == '-'
}
