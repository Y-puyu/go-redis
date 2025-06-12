package cluster

import "go-redis/interface/resp"

// ping 不需要转发
func ping(cluster *ClusterDatabase, c resp.Connection, cmdAndArgs [][]byte) resp.Reply {
	return cluster.db.Exec(c, cmdAndArgs)
}
