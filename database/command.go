package database

import (
	"strings"
)

// 命令注册表
// get、set、del、exists、keys、type、rename、renamenx、expire...
// 这里只是启动初始化该 map
// 后续只读, 不会修改, 不需要加锁, 不需要使用 sync.map
var cmdTable = make(map[string]*command)

// command 命令的执行函数和参数个数
type command struct {
	executor ExecFunc
	arity    int // allow number of args, arity < 0 means len(args) >= -arity
}

// RegisterCommand registers a new command
// arity means allowed number of cmdArgs, arity < 0 means len(args) >= -arity.
// for example: the arity of `get` is 2, `mget` is -2
// 命令注册
func RegisterCommand(name string, executor ExecFunc, arity int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: executor,
		arity:    arity,
	}
}
