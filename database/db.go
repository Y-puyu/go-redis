// Package database is a memory database with redis compatible interface
package database

import (
	"go-redis/datastruct/dict"
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
	"strings"
)

// DB stores data and execute user's commands
type DB struct {
	index int       // 当前 db 的索引
	data  dict.Dict // key -> DataEntity 使用 dict 进行底层数据存储
	// 直接将 AddAof 方法作为 DB 的成员变量
	// 这样不用将 database 的 aofHandler 传递给 DB，里面过多多于的字段，这样写封装性更好
	addAof func(CmdLine)
}

// ExecFunc is interface for command executor
// args don't include cmd line
// 命令执行函数
// db 对象, args: 命令行参数
// 最终都是返回一个 reply 接口
// 如: SET KEY VALUE
type ExecFunc func(db *DB, args [][]byte) resp.Reply

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// makeDB create DB instance
func makeDB() *DB {
	db := &DB{
		data: dict.MakeSyncDict(),
		// 这里初始化的时候一定需要给 addAof 一个空实现
		// 因为初始化 database 的时候，会初始化 aofHandler 并 执行 handler.LoadAof()
		// 将去读 aof 文件，并进行命令执行去恢复数据
		// 此时也会调用到类似 string.execSet() 这个去恢复数据
		// 那么就会调用到 db.addAof() 这个函数
		// 所以，这里的 addAof 需要一个空实现，而非不赋初值，则为 nil，调用的话将报错
		addAof: func(line CmdLine) {},
	}
	return db
}

// Exec executes command within one database
// 执行指令
func (db *DB) Exec(c resp.Connection, cmdLine CmdLine) resp.Reply {
	// PING SET GET 一般都是二维切片的第一个成员
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor

	// SET K V -> K V
	return fun(db, cmdLine[1:])
}

// validateArity 校验参数个数
// 进行如下分情况规定
// SET K V  -> arity 固定值 = 3
// EXISTS K1 K2 ... arity 变长值 = -2. 代表长度可变，且当前的参数长度为 2 个
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ---- data Access ----- */

// GetEntity returns DataEntity bind to given key
// 根据 KEY 获取一个数据实例对象
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity a DataEntity into DB
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

// Remove the given key from db
func (db *DB) Remove(key string) {
	db.data.Remove(key)
}

// Removes the given keys from db
func (db *DB) Removes(keys ...string) int {
	deleted := 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

// Flush clean database
func (db *DB) Flush() {
	db.data.Clear()
}
