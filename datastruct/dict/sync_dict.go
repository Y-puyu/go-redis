package dict

import "sync"

// SyncDict wraps a map, it is not thread safe
// 底层使用 sync_map 实现 dict 接口
// 实际就是对底层 map 的封装
// 这里是最底层的 Redis 保存数据的存储结构
// 上层是 database.Database
type SyncDict struct {
	m sync.Map
}

// MakeSyncDict makes a new map
func MakeSyncDict() *SyncDict {
	return &SyncDict{}
}

// Get returns the binding value and whether the key is exist
func (dict *SyncDict) Get(key string) (val interface{}, exists bool) {
	val, ok := dict.m.Load(key)
	return val, ok
}

// Len returns the number of dict
func (dict *SyncDict) Len() int {
	length := 0
	dict.m.Range(func(k, v interface{}) bool {
		length++
		return true
	})
	return length
}

// Put puts key value into dict and returns the number of new inserted key-value
// 返回的是成功操作的个数
func (dict *SyncDict) Put(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	dict.m.Store(key, val)
	if existed {
		return 0 // 存在时，不做任何操作，返回 0
	}
	return 1 // 不存在时，插入成功，返回 1
}

// PutIfAbsent puts value if the key is not exists and returns the number of updated key-value
func (dict *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	if existed {
		return 0
	}
	dict.m.Store(key, val)
	return 1
}

// PutIfExists puts value if the key is existed and returns the number of inserted key-value
func (dict *SyncDict) PutIfExists(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	if existed {
		dict.m.Store(key, val)
		return 1
	}
	return 0
}

// Remove removes the key and return the number of deleted key-value
func (dict *SyncDict) Remove(key string) (result int) {
	_, existed := dict.m.Load(key)
	dict.m.Delete(key)
	if existed {
		return 1
	}
	return 0
}

// ForEach traversal the dict
func (dict *SyncDict) ForEach(consumer Consumer) {
	dict.m.Range(func(key, value interface{}) bool {
		consumer(key.(string), value)
		return true
	})
}

// Keys returns all keys in dict
func (dict *SyncDict) Keys() []string {
	result := make([]string, dict.Len())
	i := 0
	dict.m.Range(func(key, value interface{}) bool {
		result[i] = key.(string)
		i++
		return true
	})
	return result
}

// RandomKeys randomly returns keys of the given number, may contain duplicated key
// 可能会存在重复的 key
func (dict *SyncDict) RandomKeys(limit int) []string {
	result := make([]string, limit)
	for i := 0; i < limit; i++ {
		// 随机取一个 key
		dict.m.Range(func(key, value interface{}) bool {
			result[i] = key.(string)
			return false
		})
	}
	return result

}

// RandomDistinctKeys randomly returns keys of the given number, won't contain duplicated key
func (dict *SyncDict) RandomDistinctKeys(limit int) []string {
	result := make([]string, limit)
	i := 0
	// 一直随机取, 取到 limit 个 key 后，就返回
	dict.m.Range(func(key, value interface{}) bool {
		result[i] = key.(string)
		i++
		if i == limit {
			return false
		}
		return true
	})
	return result
}

// Clear removes all keys in dict
// 直接用新的 map 替换掉原来的 map
// 旧的 map 会被垃圾回收
func (dict *SyncDict) Clear() {
	*dict = *MakeSyncDict()
}
