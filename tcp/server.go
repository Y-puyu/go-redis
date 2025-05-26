package tcp

/**
 * A tcp server
 */

import (
	"context"
	"fmt"
	"go-redis/interface/tcp"
	"go-redis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// Config stores tcp server properties
type Config struct {
	Address string
}

// ListenAndServeWithSignal binds port and handle requests, blocking until receive stop signal
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	// 标准做法，监听信号，收到信号后，关闭 listener，关闭 handler
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()

	// 监听端口
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))

	// 启动 tcp server
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// ListenAndServe binds port and handle requests, blocking until close
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// listen signal
	// 监听到关闭信号后，关闭 listener，关闭 handler
	// 确保 socket 连接正确释放
	go func() {
		<-closeChan
		logger.Info("shutting down...")
		_ = listener.Close() // listener.Accept() will return err immediately
		_ = handler.Close()  // close connections
	}()

	// listen port
	// 处理一些内部异常导致的 panic
	// 确保 socket 连接正确释放
	defer func() {
		// close during unexpected error
		_ = listener.Close()
		_ = handler.Close()
	}()

	// 等待组，等待所有的连接处理完成后再退出
	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {
		// 循环接收新连接
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		// handle
		logger.Info("accept link")
		// 一个协程处理一个连接
		waitDone.Add(1)
		go func() {
			// 防止 handle 出现 panic，这里最好还是使用 defer
			// 业务处理完毕，连接断开，释放资源
			defer func() {
				waitDone.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}
	// 这里需要等待所有的连接处理完成后，再退出
	waitDone.Wait()
}
