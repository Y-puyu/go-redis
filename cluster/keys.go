package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// flushDB removes all data in current database
func flushDB(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	replies := cluster.broadcast(c, args)
	// 广播执行模式，只要有一个节点的响应出错，则返回错误
	var errReply reply.ErrorReply
	for _, v := range replies {
		if reply.IsErrorReply(v) {
			errReply = v.(reply.ErrorReply)
			break
		}
	}
	if errReply == nil {
		return &reply.OkReply{}
	}
	return reply.MakeErrReply("error occurs: " + errReply.Error())
}
