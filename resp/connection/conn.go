package connection

import (
	"go-redis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// Connection represents a connection with a redis-cli
// 对每一个客户端连接的描述
type Connection struct {
	conn net.Conn // 一个连接
	// waiting until reply finished
	waitingReply wait.Wait // 一个自定义实现的具备超时退出的等待组，用于同步并发访问连接
	// lock while handler sending response
	mu sync.Mutex // 互斥锁
	// selected db
	selectedDB int // 存储当前数据库的索引
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

// RemoteAddr returns the remote network address
// 获取到远端客户端的连接地址
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Close disconnect with the client
// 等待 10s 关闭
func (c *Connection) Close() error {
	// 等待回复完毕后，关闭连接
	// 或者10s内没有回复完毕，则关闭
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

// Write sends response to client over tcp connection
// 并发安全的写入数据
func (c *Connection) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	// 加锁，防止并发写
	c.mu.Lock()
	// 添加一个等待回复的任务, 表明有协程再给客户端回写数据
	// 给 waitingReply 加1，表示正在回复
	c.waitingReply.Add(1)
	defer func() {
		// 解锁，复原
		c.waitingReply.Done()
		c.mu.Unlock()
	}()

	_, err := c.conn.Write(b)
	return err
}

// GetDBIndex returns selected db
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

// SelectDB selects a database
func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}
