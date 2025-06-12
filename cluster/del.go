package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// del atomically removes given writeKeys from cluster, writeKeys can be distributed on any node
// if the given writeKeys are distributed on different node, Del will use try-commit-catch to remove them
// del k1 k2 k3 k4...
// 通过广播群发给所有节点，每个节点回复删除的个数
// 最终返回删除的总个数
func del(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	// 借用广播逻辑，来实现 del
	replies := cluster.broadcast(c, args)
	var errReply reply.ErrorReply
	// 统计删除的个数
	var deleted int64 = 0
	for _, v := range replies {
		if reply.IsErrorReply(v) {
			errReply = v.(reply.ErrorReply)
			break
		}

		// 所有节点的回复是 int reply，表示删除的个数
		intReply, ok := v.(*reply.IntReply)
		if !ok {
			errReply = reply.MakeErrReply("error")
		}

		// 累加
		deleted += intReply.Code
	}

	// 如果错误为空，则返回删除的个数
	if errReply == nil {
		return reply.MakeIntReply(deleted)
	}
	return reply.MakeErrReply("error occurs: " + errReply.Error())
}
