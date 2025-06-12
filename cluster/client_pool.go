package cluster

import (
	"context"
	"errors"
	"github.com/jolestar/go-commons-pool/v2"
	"go-redis/resp/client"
)

// connectionFactory 连接工厂结构
// 实现 pool.PooledObjectFactory 接口
// 实现连接工厂，目的是给连接池使用，连接池可以以此来进行连接的创建、销毁、获取、返回、验证、钝化、激活等功能
type connectionFactory struct {
	Peer string
}

// MakeObject 创建连接
func (f *connectionFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	// 创建客户端连接
	c, err := client.MakeClient(f.Peer)
	if err != nil {
		return nil, err
	}
	// 启动连接到服务端
	c.Start()
	return pool.NewPooledObject(c), nil
}

// DestroyObject 销毁连接
func (f *connectionFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	// 获取连接
	c, ok := object.Object.(*client.Client)
	if !ok {
		return errors.New("type mismatch")
	}
	// 关闭连接
	c.Close()
	return nil
}

// ValidateObject 验证连接
// 无需验证
func (f *connectionFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	// do validate
	return true
}

// ActivateObject 激活连接
// 无需激活
func (f *connectionFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	// do activate
	return nil
}

// PassivateObject 钝化连接
// 无需钝化
func (f *connectionFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	// do passivate
	return nil
}
