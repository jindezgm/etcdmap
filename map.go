/*
 * @Author: jinde.zgm
 * @Date: 2020-09-03 22:42:31
 * @Descripttion:
 */

package etcdmap

import (
	"reflect"

	"go.etcd.io/etcd/clientv3"
)

// Map is the mapping of all nodes in the specific path in ETCD, Map interface design is similar to sync.Map.
type Map interface {
	Load(key string) (interface{}, bool)
	Store(key string, value interface{}) error
	Delete(key string) error
	Range(func(key string, value interface{}) bool)
	Update(key string, tryUpdate func(value interface{}) (interface{}, error)) error
	Close()
}

// NewMap create Map to map the 'path' of ETCD in 'value' type.
func NewMap(client *clientv3.Client, path string, value interface{}, codec Codec, handlers ...EventHandler) (Map, error) {
	// Check value type.
	typ := reflect.TypeOf(value)
	if typ.Kind() != reflect.Ptr {
		return nil, ErrValueTypeNotPointer
	}
	// Create cache.
	return newCache(client, path, typ.Elem(), codec, handlers...)
}
