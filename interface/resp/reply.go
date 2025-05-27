package resp

// Reply is the interface of redis serialization protocol message
// 所有的回复，都要能转成字节数组。依赖 tcp 协议
type Reply interface {
	ToBytes() []byte
}
