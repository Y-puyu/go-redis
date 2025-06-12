package database

import (
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// execGet returns string value bound to the given key
// 实现 GET 指令
// GET k1
func execGet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, ok := db.GetEntity(key)
	if !ok {
		return reply.MakeNullBulkReply()
	}
	bytes := entity.Data.([]byte)
	return reply.MakeBulkReply(bytes)
}

// execSet sets string value and time to live to the given key
// 实现 SET 指令
// SET k1 v1
func execSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	db.PutEntity(key, entity)
	// 调用 aof 功能函数，将命令写入 aof 文件中
	db.addAof(utils.ToCmdLine2("Set", args...))
	return &reply.OkReply{}
}

// execSetNX sets string if not exists
// SETNX k1 v1
func execSetNX(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	result := db.PutIfAbsent(key, entity)
	db.addAof(utils.ToCmdLine2("SetNx", args...))
	return reply.MakeIntReply(int64(result))
}

// execGetSet sets value of a string-type key and returns its old value
// GETSET 拿到 k1 的值，并设置 k1 的值为 v1
// GETSET k1 v1
func execGetSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]

	entity, exists := db.GetEntity(key)
	db.PutEntity(key, &database.DataEntity{Data: value})
	if !exists {
		return reply.MakeNullBulkReply()
	}
	old := entity.Data.([]byte)
	db.addAof(utils.ToCmdLine2("GetSet", args...))
	return reply.MakeBulkReply(old)
}

// execStrLen returns len of string value bound to the given key
// STRING 获取 key 所对应的 value 的长度
// STRLEN k -> 'value' -> 5
func execStrLen(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	old := entity.Data.([]byte)
	return reply.MakeIntReply(int64(len(old)))
}

func init() {
	RegisterCommand("get", execGet, 2)  // get k1
	RegisterCommand("set", execSet, -3) // set k1 v1 k2 v2...
	RegisterCommand("setNx", execSetNX, 3)
	RegisterCommand("getSet", execGetSet, 3)
	RegisterCommand("strLen", execStrLen, 2)
}
