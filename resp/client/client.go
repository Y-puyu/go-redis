package client

import (
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/lib/sync/wait"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"net"
	"runtime/debug"
	"sync"
	"time"
)

// Client is a pipeline mode redis client
// Redis 的客户端结构
type Client struct {
	conn        net.Conn
	pendingReqs chan *request // wait to send。发送缓冲区
	waitingReqs chan *request // waiting response。等待回复缓冲区
	ticker      *time.Ticker
	addr        string

	// 正在处理的请求数
	working *sync.WaitGroup // its counter presents unfinished requests(pending and waiting)
}

// request is a message sends to redis server
// 请求结构
type request struct {
	id        uint64     // 请求ID
	args      [][]byte   // 请求命令
	reply     resp.Reply // 响应结果
	heartbeat bool       // 是否为心跳包
	waiting   *wait.Wait // 等待回复（包含超时时间）
	err       error      // 错误记录
}

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

// MakeClient creates a new client
// 新建一个 Redis 客户端，与服务端建立连接，返回一个 Client 对象
func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:        addr,
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		working:     &sync.WaitGroup{},
	}, nil
}

// Start starts asynchronous goroutines
// 启动服务端
func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	// 协程异步发送命令，将命令发送到服务端
	go client.handleWrite()

	// 协程异步处理响应，处理服务端的数据回复
	go func() {
		err := client.handleRead()
		if err != nil {
			logger.Error(err)
		}
	}()

	// 客户端心跳保活
	go client.heartbeat()
}

// Close stops asynchronous goroutines and close connection
// 关闭连接，优雅退出，资源释放
func (client *Client) Close() {
	client.ticker.Stop()
	// stop new request
	close(client.pendingReqs)

	// wait stop process
	client.working.Wait()

	// clean
	_ = client.conn.Close()
	close(client.waitingReqs)
}

// handleConnectionError 处理连接错误
// 关闭已有连接，尝试进行重新连接
func (client *Client) handleConnectionError(err error) error {
	err1 := client.conn.Close()
	if err1 != nil {
		if opErr, ok := err1.(*net.OpError); ok {
			if opErr.Err.Error() != "use of closed network connection" {
				return err1
			}
		} else {
			return err1
		}
	}
	conn, err1 := net.Dial("tcp", client.addr)
	if err1 != nil {
		logger.Error(err1)
		return err1
	}
	client.conn = conn
	go func() {
		_ = client.handleRead()
	}()
	return nil
}

// heartbeat 发送心跳包
func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

// handleWrite 批量处理请求
func (client *Client) handleWrite() {
	// 从等待发送的 Channel 中取出待发送的 req，并将其发送到服务端
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

// Send sends a request to redis server
// 发送命令给 Redis 服务端
func (client *Client) Send(args [][]byte) resp.Reply {
	// 构建请求结构
	request := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	// 添加到等待队列
	request.waiting.Add(1)
	// 添加到工作队列
	client.working.Add(1)
	defer client.working.Done()
	// 将发送请求，添加到等待发送的 Channel
	client.pendingReqs <- request

	// 阻塞等待，且包含 3s 超时
	timeout := request.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return reply.MakeErrReply("server time out")
	}
	if request.err != nil {
		return reply.MakeErrReply("request failed")
	}
	return request.reply
}

// doHeartbeat 发送心跳包
func (client *Client) doHeartbeat() {
	request := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- request
	request.waiting.WaitWithTimeout(maxWait)
}

// doRequest 发送请求
func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	// 将发送命令转化为 RESP 协议格式
	re := reply.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()
	_, err := client.conn.Write(bytes)
	// 如果出现错误，则重试 3 次
	i := 0
	for err != nil && i < 3 {
		// 处理错误
		// 可能是连接错误，进行重新连接
		err = client.handleConnectionError(err)
		if err == nil {
			_, err = client.conn.Write(bytes)
		}
		i++
	}
	if err == nil {
		// 命令成功发送，将请求对象，添加到等待回复缓冲区
		client.waitingReqs <- req
	} else {
		// 发送报错，记录错误，等待缓冲区 -1
		req.err = err
		req.waiting.Done()
	}
}

// finishRequest 请求处理完毕
func (client *Client) finishRequest(reply resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()
	// 从等待回复缓冲区中取出回复对象
	request := <-client.waitingReqs
	if request == nil {
		return
	}
	// 添加回复数据
	// 这里并没有再回复给服务端，仅是拿到服务端的回复数据后，做记录，并没有其余业务处理
	request.reply = reply
	// 异步回复完成，释放工作队列
	if request.waiting != nil {
		request.waiting.Done()
	}
}

// handleRead 批量处理响应
func (client *Client) handleRead() error {
	// 异步解析服务端回复的数据流，返回 Channel
	ch := parser.ParseStream(client.conn)
	// 循环处理数据
	for payload := range ch {
		if payload.Err != nil {
			// 请求报错，返回错误信息，完成本次请求
			client.finishRequest(reply.MakeErrReply(payload.Err.Error()))
			continue
		}
		// 正常返回数据，完成本次响应请求
		client.finishRequest(payload.Data)
	}
	return nil
}
