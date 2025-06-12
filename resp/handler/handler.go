package handler

/*
 * A tcp.RespHandler implements redis protocol
 */

import (
	"context"
	"errors"
	"go-redis/cluster"
	"go-redis/config"
	"go-redis/database"
	databaseface "go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/sync/atomic"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"net"
	"strings"
	"sync"
)

var (
	// 未知错误
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

// RespHandler implements tcp.Handler and serves as a redis handler
// 处理客户端请求
type RespHandler struct {
	activeConn sync.Map              // *client -> placeholder 存储当前活跃的连接
	db         databaseface.Database // 抽象接口对象，表示与此响应处理程序关联的数据库
	closing    atomic.Boolean        // 表示服务器是否正在关闭，拒绝其他 goroutine 的请求
}

// MakeHandler creates a RespHandler instance
func MakeHandler() *RespHandler {
	var db databaseface.Database
	if config.Properties.Self != "" && len(config.Properties.Peers) > 0 {
		// 使用集群版本的实现
		db = cluster.MakeClusterDatabase()
	} else {
		db = database.NewStandaloneDatabase()
	}
	return &RespHandler{
		db: db,
	}
}

func (h *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

// Handle receives and executes redis commands
// 处理客户端请求，直到连接关闭为止
func (h *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	// 如果已经正在关闭了，就拒绝新的客户端连接
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
	}

	// 到来一个新的客户端连接，并添加到活跃连接列表中
	client := connection.NewConn(conn)
	h.activeConn.Store(client, struct{}{})

	// 异步流式解析客户端请求
	ch := parser.ParseStream(conn)
	for payload := range ch {
		// error 逻辑
		if payload.Err != nil {
			if payload.Err == io.EOF || // io.EOF 代表客户端关闭连接
				errors.Is(payload.Err, io.ErrUnexpectedEOF) || // io.ErrUnexpectedEOF 代表客户端发送了一个不完整的请求
				// 使用了被关闭的连接
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// protocol err
			// 构造错误信息，返回给客户端
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		// 如果没有数据，则忽略
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}

		// 如果数据不是 MultiBulkReply，则忽略
		r, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}

		// exec 逻辑
		// 执行命令
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

// Close stops handler
// 关闭协议层，关闭整个 Redis
func (h *RespHandler) Close() error {
	logger.Info("handler shutting down...")
	// 设置正在关闭的标志位
	h.closing.Set(true)
	// TODO: concurrent wait
	// 遍历所有活跃连接，并关闭
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
