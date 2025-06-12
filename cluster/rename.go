package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// rename renames a key, the origin and the destination must within the same node
// eg: rename k1 k2
func rename(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	// 一定是 3个 参数
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[1])  // 修改前的 key
	dest := string(args[2]) // 修改后的 key

	srcPeer := cluster.peerPicker.PickNode(src)   // 拿到 原节点 key，通过一致性哈希找到对应的节点
	destPeer := cluster.peerPicker.PickNode(dest) // 拿到 目标 key，通过一致性哈希找到对应的节点

	// 如果 原节点和 目标节点 不同，简单处理，直接报错
	// 也可以实现另一套逻辑，将原节点数据删除，再去新节点数据覆盖
	if srcPeer != destPeer {
		return reply.MakeErrReply("ERR rename must within one slot in cluster mode")
	}
	// 调用转发
	return cluster.relay(srcPeer, c, args)
}
