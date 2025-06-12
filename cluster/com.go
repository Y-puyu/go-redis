package cluster

import (
    "context"
    "errors"
    "go-redis/interface/resp"
    "go-redis/lib/utils"
    "go-redis/resp/client"
    "go-redis/resp/reply"
    "strconv"
)

/*
不同的命令，集群的各个节点中要表现出不同的行为
1. PING（本地执行）
	用户访问节点，测试的是 该节点 是否正常。与其他节点无关。
	可以直接调用 standalone_database 的 Exec 方法，回复 PONG 即可

2. SET、GET（命令转发）
	用户访问单个节点，执行 SET、GET 操作的话，需要通过一致性哈希管理器获取到对应的节点，然后转发给该节点执行。
	转发需要用到连接池，将自身节点伪装成 Redis 客户端，发送用户命令到 目标节点，再将 目标节点的回复转发给用户。

3. FLUSHDB（命令群发）
	用户希望清空数据库，那么需要将集群中的，所有节点的数据都清空。
    这里就是命令群发。
*/

// getPeerClient gets peer client
// 通过连接地址，在连接池中拿到一个连接对象
func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
    factory, ok := cluster.peerConnection[peer]
    if !ok {
        return nil, errors.New("connection factory not found")
    }

    // 借一个连接对象
    // 注意：该连接对象需要还到连接池中。否则将进行连接泄漏或者连接池耗尽
    raw, err := factory.BorrowObject(context.Background())
    if err != nil {
        return nil, err
    }

    conn, ok := raw.(*client.Client)
    if !ok {
        return nil, errors.New("connection factory make wrong type")
    }
    return conn, nil
}

// returnPeerClient 业务处理完毕后，给连接池中还回去一个连接对象
func (cluster *ClusterDatabase) returnPeerClient(peer string, peerClient *client.Client) error {
    connectionFactory, ok := cluster.peerConnection[peer]
    if !ok {
        return errors.New("connection factory not found")
    }
    return connectionFactory.ReturnObject(context.Background(), peerClient)
}

// relay relays command to peer
// select db by c.GetDBIndex()
// cannot call Prepare, Commit, execRollback of self node
// 实现转发
func (cluster *ClusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
    // 如果需要转发的地址是本身，则直接执行命令
    if peer == cluster.self {
        // to self db
        return cluster.db.Exec(c, args)
    }

    // 从连接池中获取连接对象
    peerClient, err := cluster.getPeerClient(peer)
    if err != nil {
        return reply.MakeErrReply(err.Error())
    }
    // 归还连接
    defer func() {
        _ = cluster.returnPeerClient(peer, peerClient)
    }()

    // 注意：发送命令到其他节点时，需要在相同的 DB 下执行命令！！！
    peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))
    // 发送命令到其他节点
    return peerClient.Send(args)
}

// broadcast broadcasts command to all node in cluster
// 实现广播。广播的返回值是每个节点的回复，所以返回值是一个 map
func (cluster *ClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply {
    result := make(map[string]resp.Reply)
    for _, node := range cluster.nodes {
        result[node] = cluster.relay(node, c, args)
    }
    return result
}
