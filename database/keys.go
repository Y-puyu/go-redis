package database

import (
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/lib/wildcard"
	"go-redis/resp/reply"
)

// execDel removes a key from db
// DEL 命令
// DEL K1 K2 K3
// 实际上执行的时候, args 就只有 K1 K2 K3 指令了, DEL 在前面已经被切掉了
func execDel(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	deleted := db.Removes(keys...)

	// 如果真的删除了数据，则添加 AOF
	if deleted > 0 {
		db.addAof(utils.ToCmdLine2("Del", args...))
	}

	// 包装成 RESP 协议的整数返回形式
	return reply.MakeIntReply(int64(deleted))
}

// execExists checks if a is existed in db
// EXISTS K1 K2 K3
func execExists(db *DB, args [][]byte) resp.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result++
		}
	}

	// 包装成 RESP 协议的整数返回形式
	return reply.MakeIntReply(result)
}

// execFlushDB removes all data in current db
// FLUSHDB
func execFlushDB(db *DB, args [][]byte) resp.Reply {
	db.Flush()
	// 包装成 RESP 协议的 OK 返回形式
	return &reply.OkReply{}
}

// execType returns the type of entity, including: string, list, hash, set and zset
// 目前仅支持 string 类型
// TYPE K1
// 这里 args 实际上只有 K1, 因为 TYPE 在前面已经被切掉了
func execType(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeStatusReply("none")
	}

	// 后续可实现其他类型
	switch entity.Data.(type) {
	case []byte: // string 类型就是按照 []byte 来存储的
		return reply.MakeStatusReply("string")
	}
	return &reply.UnknownErrReply{}
}

// execRename a key
// RENAME K1 K2
func execRename(db *DB, args [][]byte) resp.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[0])
	dest := string(args[1])

	entity, ok := db.GetEntity(src)
	if !ok {
		return reply.MakeErrReply("no such key")
	}

	// 更新 dest、删除 src
	db.PutEntity(dest, entity)
	db.Remove(src)
	db.addAof(utils.ToCmdLine2("RenameNx", args...))
	return &reply.OkReply{}
}

// execRenameNx a key, only if the new key does not exist
// RENAMENX K1 K2
// 检查 K2 是否存在, 如果存在则返回 0, 什么也不操作
// 如果 K2 不存在, 则将 K1 改名为 K2, 删除 K1
func execRenameNx(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])
	dest := string(args[1])

	// 如果 K2 存在，则直接返回 0，表示什么也不做
	_, ok := db.GetEntity(dest)
	if ok {
		return reply.MakeIntReply(0)
	}

	// 如果 K1 存在, 则将 K1 改名为 K2, 删除 K1
	entity, ok := db.GetEntity(src)
	if !ok {
		return reply.MakeErrReply("no such key")
	}
	// 删除 K1
	db.Removes(src)
	// 插入 K2
	db.PutEntity(dest, entity)
	db.addAof(utils.ToCmdLine2("Rename", args...))
	// 返回操作数：1
	return reply.MakeIntReply(1)
}

// execKeys returns all keys matching the given pattern
func execKeys(db *DB, args [][]byte) resp.Reply {
	pattern := wildcard.CompilePattern(string(args[0]))
	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		// 调用 wildcard.IsMatch, 进行通配符匹配
		if pattern.IsMatch(key) {
			result = append(result, []byte(key))
		}
		return true
	})

	// 返回所有匹配的 KEY
	return reply.MakeMultiBulkReply(result)
}

func init() {
	RegisterCommand("Del", execDel, -2)          // DEL K1... 至少两个参数、变长
	RegisterCommand("Exists", execExists, -2)    // EXISTS K1... 至少两个参数、变长
	RegisterCommand("FlushDB", execFlushDB, -1)  // FLUSHDB 命令, 其实是固定参数 1 个。但是这里为了兼容性, 允许变长, 如 FLUSHDB a b c, 但也只执行 FLUSHDB 命令, 这也是 -1 的作用
	RegisterCommand("Type", execType, 2)         // TYPE K1 固定两个参数
	RegisterCommand("Rename", execRename, 3)     // RENAME K1 K2 固定三个参数
	RegisterCommand("RenameNx", execRenameNx, 3) // RENAMENX K1 K2 固定三个参数
	RegisterCommand("Keys", execKeys, 2)         // KEYS PATTERN 固定两个参数
}
