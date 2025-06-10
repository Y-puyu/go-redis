package aof

import (
	"go-redis/config"
	databaseface "go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"os"
	"strconv"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16
)

// payload AOF 写缓冲的数据结构
type payload struct {
	cmdLine CmdLine // 用户指令
	dbIndex int     // 数据库索引
}

// AofHandler receive msgs from channel and write to AOF file
// AOF 文件处理器。负责 AOF 文件的读写
type AofHandler struct {
	db          databaseface.Database // 持有 database
	aofChan     chan *payload         // AOF 写文件缓冲区
	aofFile     *os.File              // AOF 文件句柄
	aofFilename string                // AOF 文件名称
	currentDB   int                   // 上一次写指令的数据库索引
}

// NewAOFHandler creates a new aof.AofHandler
// 创建一个 AOF 文件处理器
func NewAOFHandler(db databaseface.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	// 从配置中获取 AOF 文件名称
	handler.aofFilename = config.Properties.AppendFilename
	handler.db = db
	// 恢复数据
	handler.LoadAof()
	// 打开文件，追加写入，文件不存在则创建，以读写方式打开，权限 0600
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	// 保存文件句柄，初始化 handler，并初始化 AOF 写入缓冲区
	handler.aofFile = aofFile
	handler.aofChan = make(chan *payload, aofQueueSize)

	// 异步落盘
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

// AddAof send command to aof goroutine through channel
// 将用户指令塞到 Channel 缓冲区中
// 这里一定需要记录 db 索引，因为 AOF 文件中写入的命令，可能不是当前数据库的
func (handler *AofHandler) AddAof(dbIndex int, cmdLine CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
	}
}

// handleAof listen aof channel and write into file
// 将缓冲区的数据写入文件，落盘
func (handler *AofHandler) handleAof() {
	// serialized execution
	// 初始化 数据库索引为 0，默认在 0 数据库工作
	handler.currentDB = 0
	for p := range handler.aofChan {
		// 如果当前数据库索引和上一条指令的索引不一致，则需要切换数据库
		// 否则不进行数据库切换
		if p.dbIndex != handler.currentDB {
			// select db
			// eg: SELECT 0
			// 添加 select DB 记录，并写入文件
			// 这里需要多次的结构转换需注意！
			data := reply.MakeMultiBulkReply(
				utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
				continue // skip this command
			}
			// 记录当前数据库索引，与下一次指令的数据库索引做比较
			handler.currentDB = p.dbIndex
		}

		// 两个情况
		// 1. 数据库索引切换写入完成，开始写入指令
		// 2. 数据库不需切换，直接写入指令
		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
		}
	}
}

// LoadAof read aof file
// 将磁盘中的 AOF 文件再加载到内存中
// AOF 文件本来就是严格的 RESP 协议格式
// 所以可以直接读取 AOF 文件，再把这些指令重新执行一遍即可
func (handler *AofHandler) LoadAof() {
	// 只读方式打开 aof 文件
	file, err := os.Open(handler.aofFilename)
	if err != nil {
		logger.Warn(err)
		return
	}
	// 这里需要关闭文件句柄，在 NewAOFHandler 中打开的文件不需要关闭，生命周期等于进程生命周期
	defer file.Close()
	ch := parser.ParseStream(file)
	// 造一个假连接，仅为了保存 dbIndex
	fakeConn := &connection.Connection{} // only used for save dbIndex
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF { // 知识读到 EOF 而已，说明文件已经读取完毕了，正常退出即可
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		// 错误情况下，可能没有数据，这里不能处理，直接扔掉该行即可
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}

		// 所需的 Data 类型仅为 MultiBulkReply
		// 这里才是二维字节切片，所有的用户命令都是 MultiBulkReply 格式的
		// 对于 +ok\r\n 这些非 MultiBulkReply 格式，则忽略
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		ret := handler.db.Exec(fakeConn, r.Args)
		if reply.IsErrorReply(ret) {
			logger.Error("exec err", err)
		}
	}
}
