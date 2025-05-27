package resp

// Connection represents a connection with redis client
// Redis 协议层代表一个连接
type Connection interface {
	Write([]byte) error // 客户端回复消息
	GetDBIndex() int    // 1-16个 db，返回当前使用的 db
	SelectDB(int)       // 选择 db，切换数据库
}
