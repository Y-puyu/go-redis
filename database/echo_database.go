package database

import (
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
)

type EchoDatabase struct {
}

func NewEchoDatabase() *EchoDatabase {
	return &EchoDatabase{}
}

func (e EchoDatabase) Exec(client resp.Connection, args [][]byte) resp.Reply {
	// 将 Redis 客户端的 RESP 协议解析为命令参数后
	// 再将 命令参数拼装为 RESP 协议的回复，进行回显
	// 用于测试 RESP 协议的编解码 是否正常
	return reply.MakeMultiBulkReply(args)

}

func (e EchoDatabase) AfterClientClose(c resp.Connection) {
	logger.Info("EchoDatabase AfterClientClose")
}

func (e EchoDatabase) Close() {
	logger.Info("EchoDatabase Close")

}
