package database

import (
	"fmt"
	"go-redis/aof"
	"go-redis/config"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"runtime/debug"
	"strconv"
	"strings"
)

// StandaloneDatabase is a set of multiple database set
// 一个单机版的 Redis 数据库
type StandaloneDatabase struct {
	dbSet []*DB
	// handle aof persistence
	// 创建一个 aofHandler，用于执行 aof 相关业务
	aofHandler *aof.AofHandler
}

// NewStandaloneDatabase creates a standaloneDatabase redis database,
func NewStandaloneDatabase() *StandaloneDatabase {
	mdb := &StandaloneDatabase{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	// 创建指定数量的 db 切片
	// 并循环进行初始化
	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		mdb.dbSet[i] = singleDB
	}

	// 初始化 Databases 时，并创建 aofHandler
	if config.Properties.AppendOnly {
		handler, err := aof.NewAOFHandler(mdb)
		if err != nil {
			panic(err)
		}
		mdb.aofHandler = handler

		// 给每个 db 添加 addAof 方法
		for _, db := range mdb.dbSet {
			// go1.22 版本后，db 不会再产生闭包问题
			db.addAof = func(line CmdLine) {
				mdb.aofHandler.AddAof(db.index, line)
			}
		}
	}
	return mdb
}

// Exec executes command
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
// 执行数据库相关的核心业务方法
// cmdLine 有两种情况
// 第一种: 和某一个 db 相关的 set k v、get k
// 第二种: select 2 指定选择某个数据库的
func (mdb *StandaloneDatabase) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
		}
	}()

	// 取出第一个参数
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "select" {
		if len(cmdLine) != 2 {
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(c, mdb, cmdLine[1:])
	}
	// normal commands
	dbIndex := c.GetDBIndex()
	selectedDB := mdb.dbSet[dbIndex]
	return selectedDB.Exec(c, cmdLine)
}

// Close graceful shutdown database
// 不需要实现
func (mdb *StandaloneDatabase) Close() {}

// AfterClientClose is called when client closed
// 不需要实现
func (mdb *StandaloneDatabase) AfterClientClose(c resp.Connection) {}

// execSelect 选择数据库
func execSelect(c resp.Connection, mdb *StandaloneDatabase, args [][]byte) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return reply.MakeOkReply()
}
