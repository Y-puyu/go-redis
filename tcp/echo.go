package tcp

/**
 * A echo server to test whether the server is functioning normally
 */

import (
	"bufio"
	"context"
	"go-redis/lib/logger"
	"go-redis/lib/sync/atomic"
	"go-redis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

// EchoHandler echos received line to client, using for test
// 实现一个简单的回显服务器，用来测试 tcp 服务器框架是否正常
// 业务引擎对象。接收所有的连接，并保存起来，然后对每个连接进行业务处理
type EchoHandler struct {
	activeConn sync.Map       // 保存所有活跃的连接
	closing    atomic.Boolean // 连接是否正在关闭
}

// MakeHandler creates EchoHandler
func MakeHandler() *EchoHandler {
	return &EchoHandler{}
}

// EchoClient is client for EchoHandler, using for test
// 当前连接的客户端信息。
// 一个结构体对象，就是一个客户端
type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait
}

// Close connection
// 这里实现一个超时的关闭连接，如果超过 10 秒还没有处理完业务，就关闭该客户端连接
func (c *EchoClient) Close() error {
	c.Waiting.WaitWithTimeout(10 * time.Second)
	_ = c.Conn.Close()
	return nil
}

// Handle echos received line to client
func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	// 如果已经正在关闭了，就拒绝新的客户端连接
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
	}

	// 到来一个新的客户端连接，并添加到活跃连接列表中
	client := &EchoClient{
		Conn: conn,
	}
	h.activeConn.Store(client, struct{}{})

	// 开始进行业务处理
	// 生成一个读取器
	reader := bufio.NewReader(conn)
	for {
		// may occurs: client EOF, client timeout, server early close
		// 读取一行数据，出错则关闭连接
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF { // 收到 EOF 意味着客户端退出了，引擎层直接删除该连接即可，无需再关闭
				logger.Info("connection close")
				h.activeConn.Delete(client)
			} else { // 其他错误，打印日志，并关闭连接
				logger.Warn(err)
			}
			return
		}
		// 记录该连接正在处理中
		client.Waiting.Add(1)
		// 取出数据，回发数据
		b := []byte(msg)
		_, _ = conn.Write(b)
		// 记录该连接处理完毕
		client.Waiting.Done()
	}
}

// Close stops echo handler
func (h *EchoHandler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true) // 设置关闭标志位
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*EchoClient)
		_ = client.Close()
		return true // 继续遍历
	})
	return nil
}
