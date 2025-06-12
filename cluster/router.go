package cluster

import "go-redis/interface/resp"

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	routerMap["ping"] = ping         // PING
	routerMap["select"] = execSelect // SELECT 1

	routerMap["del"] = del // DEL k1 k2 k3

	routerMap["exists"] = defaultFunc // EXISTS k1
	routerMap["type"] = defaultFunc   // TYPE k1
	routerMap["set"] = defaultFunc    // SET k1 v1
	routerMap["setnx"] = defaultFunc  // SETNX k1 v1
	routerMap["get"] = defaultFunc    // GET k1
	routerMap["getset"] = defaultFunc // GETSET k1 v1

	routerMap["rename"] = rename   // RENAME k1 k2
	routerMap["renamenx"] = rename // RENAMENX k1 k2 这个只负责转发，不需要做任何处理

	routerMap["flushdb"] = flushDB // FLUSHDB

	return routerMap
}

// relay command to responsible peer, and return its reply to client
// 默认的转发方法，大多数的命令可能都需要转发
// GET Key / SET K1 V1
func defaultFunc(cluster *ClusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	// 只需要拿到 key，通过一致性哈希就可以找到对应的节点
	key := string(args[1])
	peer := cluster.peerPicker.PickNode(key)
	return cluster.relay(peer, c, args)
}
