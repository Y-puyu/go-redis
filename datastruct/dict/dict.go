package dict

// Consumer is used to traversal dict, if it returns false the traversal will be break
// 遍历所有 k-v, 并针对每一个 k-v，调用 consumer, 根据返回值判断是否继续遍历
type Consumer func(key string, val interface{}) bool

// Dict is interface of a key-value data structure
// 定义 Dict 接口，描述字典的能力
// 后续如果需要更换 Dict 的实现，则只需要实现这个接口，不需要修改其他代码
type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int
	Put(key string, val interface{}) (result int)
	PutIfAbsent(key string, val interface{}) (result int) // 如果没有，则设置。SET NX
	PutIfExists(key string, val interface{}) (result int) // 如果有，则设置。
	Remove(key string) (result int)
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string         //  随机返回 limit 个 key
	RandomDistinctKeys(limit int) []string // 随机返回 limit 个 不同的 key
	Clear()
}
