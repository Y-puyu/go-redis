package database

import (
	"go-redis/interface/resp"
)

// CmdLine is alias for [][]byte, represents a command line
// 一行命令数据类型为 [][]byte，每个 []byte 代表一个参数
type CmdLine [][]byte

// Database is the interface for redis style storage engine
type Database interface {
	Exec(client resp.Connection, args [][]byte) resp.Reply // 数据库执行命令
	AfterClientClose(c resp.Connection)                    // 处理客户端关闭连接请求
	Close()                                                // 关闭数据库
}

// DataEntity stores data bound to a key, including a string, list, hash, set and so on
// 数据库中的数据实体，可以表示为任意类型
// 使用空接口做的封装，方便日后拓展
type DataEntity struct {
	Data interface{}
}
