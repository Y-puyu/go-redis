// Package cluster provides a server side cluster which is transparent to client. You can connect to any node in the cluster to access all data in the cluster
package cluster

import (
	"context"
	"fmt"
	pool "github.com/jolestar/go-commons-pool/v2"
	"go-redis/config"
	"go-redis/database"
	databaseface "go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/consistenthash"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"runtime/debug"
	"strings"
)

// ClusterDatabase represents a node of godis cluster
// it holds part of data and coordinates other nodes to finish transactions
// Redis 集群结构
// 一个集群中的一个节点，就是一个单节点的 StandaloneDatabase 结构
type ClusterDatabase struct {
	self           string                      // 自身节点地址
	nodes          []string                    // 整个集群中的所有节点地址
	peerPicker     *consistenthash.NodeMap     // 节点选择器，使用一致性哈希管理器
	peerConnection map[string]*pool.ObjectPool // 节点连接池。需要实现连接的创建、销毁、获取、返回等功能
	db             databaseface.Database       // 集群所在节点自身的数据库
}

// MakeClusterDatabase creates and starts a node of cluster
func MakeClusterDatabase() *ClusterDatabase {
	// 初始化集群中的单节点数据库
	cluster := &ClusterDatabase{
		self:           config.Properties.Self,
		db:             database.NewStandaloneDatabase(),
		peerPicker:     consistenthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
	}

	// 初始化节点数量。容量：自身 + 其他 Redis 节点
	// 将自身信息也添加到集群节点列表中
	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	nodes = append(nodes, config.Properties.Self)
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	cluster.nodes = nodes

	// 将所有的节点添加到一致性哈希管理器中
	cluster.peerPicker.AddNode(nodes...)

	// 初始化连接池
	// 对每一个兄弟节点，都传入连接工厂
	// 连接工厂传入之后，连接池会自动 新建 & 维护 连接个数
	// 这里使用默认的连接池配置，会根据每个兄弟节点之间新建 8 个空闲连接
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}
	return cluster
}

// CmdFunc represents the handler of a redis command
// 声明 集群命令处理函数
type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdAndArgs [][]byte) resp.Reply

// Close stops current node of cluster
func (cluster *ClusterDatabase) Close() {
	cluster.db.Close()
}

var router = makeRouter()

// Exec executes command on cluster
// 集群的命令执行，代替单机版的命令执行
func (cluster *ClusterDatabase) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	// 拿到第一个指令名称
	cmdName := strings.ToLower(string(cmdLine[0]))
	// 拿到方法
	cmdFunc, ok := router[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	result = cmdFunc(cluster, c, cmdLine)
	return
}

// AfterClientClose does some clean after client close connection
func (cluster *ClusterDatabase) AfterClientClose(c resp.Connection) {
	cluster.db.AfterClientClose(c)
}
