package parser

import (
	"bufio"
	"errors"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

// Payload stores redis.Reply or error
// 解析用户发送过来的数据
// 这里对于 Data 都使用 resp.Reply 接口做抽象
// 不论是服务器发送给客户端，还是客户端发送给服务器，都使用 resp.Reply 接口做抽象
// 因为数据结构是同一套逻辑
type Payload struct {
	Data resp.Reply
	Err  error
}

// ParseStream reads data from io.Reader and send payloads through channel
// 异步解析数据流。实现异步解析，异步返回。
// 这里的返回值也是一个只读的 channel，读取这个 channel 就可以异步非阻塞的得到解析结果
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// 存储当前数据读取的解析状态
type readState struct {
	readingMultiLine  bool     // 正在读取的是单行还是多行数据
	expectedArgsCount int      // 预期读取的参数个数
	msgType           byte     // 当前读取的消息类型
	args              [][]byte // 表示已经读取的参数列表。例如 set k v 就有三个，每一个都是 []byte
	bulkLen           int64    // 正在读取的块数据的长度
}

// finished 判断解析是否完成
func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// parse0 解析 tcp 到来的数据
func parse0(reader io.Reader, ch chan<- *Payload) {
	// 这里捕获 panic，避免带崩其他协程
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()

	// 创建一个 bufReader，其是一个带有缓冲区的读取器
	// 用于从 reader 中读取数据流
	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	for {
		// read line
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)
		if err != nil {
			if ioErr { // encounter io err, stop read
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
			// protocol err, reset read state
			ch <- &Payload{
				Err: err,
			}
			state = readState{}
			continue
		}

		// 判断是否为多行解析模式
		// 解析每一行数据
		// 如果不是在读取一个多行数据，就是在读取一个新的数据行
		// 新数据行的第一个字符，就是数据类型
		// 根据不同的数据类型，进行数据解析
		if !state.readingMultiLine {
			// receive new response
			// 如果还没在多行解析模式下，且用户发来的第一个字符是 *
			// 则表示该数据是多行数据
			// 则由 parseMultiBulkHeader 解析，并将 state 改为多行解析模式
			if msg[0] == '*' {
				// multi bulk reply
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // reset state
					continue
				}
				// 特殊情况：如果是 *0 的话，则表示一个空的多行数据
				// 则返回一个空多行数据
				// 继续解析下一行数据即可，重置 state
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{} // reset state
					continue
				}
			} else if msg[0] == '$' { // bulk reply
				// $4\r\nPING\r\n
				// 在非多行模式的情况下，需要调用 parseBulkHeader 方法
				// 将 state 改变为多行模式，并且进行数据解析
				// 实际上 $4 和 PING 在这里是看作两行的
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // reset state
					continue
				}
				// $-1\r\n 说明是空字符串
				if state.bulkLen == -1 { // null bulk reply
					ch <- &Payload{
						Data: &reply.NullBulkReply{},
					}
					state = readState{} // reset state
					continue
				}
			} else {
				// single line reply
				// 解析单行数据，不会更改解析器 state 的状态
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{} // reset state
				continue
			}
		} else {
			// receive following bulk reply
			// 已经是多行模式了
			// 这里还是在读取多行数据
			// 现在每一行就是一个独立的字符串进行处理即可
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readState{} // reset state
				continue
			}
			// if sending finished
			// 判断多行数据是否已经读取完毕
			// 构建响应 reply
			// 通过 ch 进行数据发送
			if state.finished() {
				var result resp.Reply
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0]) // 单行字符串，注意传参方式
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

// readLine 读取一行数据
// 返回读取到的数据，是否遇到 io 错误，以及错误信息
// 情况1: 没有预设个数，直接按照 \r\n 进行切分
// 情况2: 之前读到 $ 数字，严格读取字符个数。防止 \r\n 是数据内容的一部分
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error
	// 如果 bulkLen == 0 则函数一直读取，直到遇到 \n 为止
	// 情况1: 待读取的字节数不确定，没有预设个数
	// *3\r\n$3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n
	// 先读取 *3\r\n, 则返回 *3
	if state.bulkLen == 0 {
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		// 如果倒数第二个字符不是 \r 的话，就不是以 \r\n 结尾的，返回协议错误
		if len(msg) < 2 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else {
		// 否则，仅读取 bulkLen+\r\n 个字符即可
		// 待读取的字节数确定, 之前已经读到了 $数字
		// $3 SET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n
		// 已经读到了 $3, 那么需要读到 set\r\n 总共 3+2=5 个字符
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		// 判断是否为 \r\n 结尾
		if len(msg) < 2 ||
			msg[len(msg)-2] != '\r' ||
			msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		// 读取完毕，将 bulkLen 置 0, 该行已经读取完毕
		state.bulkLen = 0
	}
	// 返回读取到的 msg，是否遇到 io 错误，以及错误信息
	return msg, false, nil
}

// parseMultiBulkHeader 解析多行字符串(数组)的首行头部信息
// 针对 *3\r\n$3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n 例子而言
// 首先由 readLine 读取到 *3\r\n, 接下来会由  parseMultiBulkHeader 进行解析
// 并维护 state *readState 中的状态
// 多行字符串是有多行构成的
// 每行都是一个简单的字符串
// 多行字符串以 * 符号开头，后接一个数字，表示多行字符串的行数
// SET KEY VALUE
// *3\r\n$3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64

	// 跳过第一个符号位，跳过最后的 \r\n, 计算实际的内容长度
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	// 如果是 0，那就是一个空的字符串
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 { // 非零，则说明是非空的多行字符串
		// first line of multi bulk reply
		state.msgType = msg[0]                      // msgType 记录当前读取的回复类型
		state.readingMultiLine = true               // 读取多行字符串标记置为 true
		state.expectedArgsCount = int(expectedLine) // 记录当前读取的参数个数
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// parseBulkHeader 解析简单字符串的头部信息
// ABC
// $3\r\nABC\r\n
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	// 简单字符串先读取 bulkLen，获取到字符串长度后，再读取数据
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 { // null bulk
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true     // 将多行字符串标记位置为 true
		state.expectedArgsCount = 1       // 表示该字符串只包含一个元素，就是这个简单字符串
		state.args = make([][]byte, 0, 1) // 这里创建一个数组，用于存储该简单字符串，就只包含一个元素
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// parseSingleLineReply 解析单行回复
// 解析 +OK\r\n -ERR\r\n :5\r\n 这三种单行回复
func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	// 去掉末尾的 \r\n
	str := strings.TrimSuffix(string(msg), "\r\n")

	// 声明一个 resp.Reply 接口对象
	var result resp.Reply
	switch msg[0] {
	case '+': // status reply 正常的状态回复
		result = reply.MakeStatusReply(str[1:])
	case '-': // err reply 错误回复
		result = reply.MakeErrReply(str[1:])
	case ':': // int reply 整数回复
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = reply.MakeIntReply(val)
	}
	return result, nil
}

// read the non-first lines of multi bulk reply or bulk reply
// 用于读取 Redis 返回的多行字符串和简单字符串的每一行
// 都是以 $ 开头的
// 例如：SET KEY VALUE
// 如：*3\r\n$3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n
// 情况1: $3\r\n
// 情况2: SET\r\n
func readBody(msg []byte, state *readState) error {
	// 先去除掉最后的 \r\n
	line := msg[0 : len(msg)-2]
	var err error
	if line[0] == '$' { // 如果是 $ 开头的话，就是一个 bulk reply
		// bulk reply
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen <= 0 { // null bulk in multi bulks
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		// 如果不是以 $ 开头的话，就是一个简单字符串
		// 直接将 line 添加到 args 中即可
		state.args = append(state.args, line)
	}
	return nil
}
