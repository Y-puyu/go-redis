package consistenthash

import (
	"hash/crc32"
	"sort"
)

// HashFunc defines function to generate hash code
// 哈希函数 结构定义
type HashFunc func(data []byte) uint32

// NodeMap stores nodes and you can pick node from NodeMap
// 存放所有的 hash 节点，等价于 哈希环3
type NodeMap struct {
	hashFunc    HashFunc       // 传入哈希函数
	nodeHashs   []int          // 存放节点的哈希值，需要有序，方便查询
	nodehashMap map[int]string // 节点与节点信息对应表，可为：nodeHashs:nodeIp
}

// NewNodeMap creates a new NodeMap
func NewNodeMap(fn HashFunc) *NodeMap {
	m := &NodeMap{
		hashFunc:    fn,
		nodehashMap: make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty returns if there is no node in NodeMap
func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashs) == 0
}

// AddNode add the given nodes into consistent hash circle
// 增加节点
func (m *NodeMap) AddNode(keys ...string) {
	for _, key := range keys {
		// 如果没有节点信息，则直接跳过
		if key == "" {
			continue
		}
		hash := int(m.hashFunc([]byte(key)))
		m.nodeHashs = append(m.nodeHashs, hash)
		m.nodehashMap[hash] = key
	}
	// 增加节点后需要排序，便于查找
	sort.Ints(m.nodeHashs)
}

// PickNode gets the closest item in the hash to the provided key.
// 根据 key 进行节点选择，获取最接近的节点
func (m *NodeMap) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}

	// 对 key 进行哈希计算
	hash := int(m.hashFunc([]byte(key)))

	// Binary search for appropriate replica.
	// 二分查找，找到最近的小于当前 hash 的节点序号
	idx := sort.Search(len(m.nodeHashs), func(i int) bool {
		return hash <= m.nodeHashs[i]
	})

	// Means we have cycled back to the first replica.
	// 如果 hash 属于最大的序号，则需要将其置为第一个节点，完成哈希环的闭环
	if idx == len(m.nodeHashs) {
		idx = 0
	}

	return m.nodehashMap[m.nodeHashs[idx]]
}
