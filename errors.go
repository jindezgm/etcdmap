/*
 * @Author: jinde.zgm
 * @Date: 2020-09-03 22:49:33
 * @Descripttion:
 */
package etcdmap

import "errors"

var (
	// ErrValueTypeNotPointer is return if the value type passed in is not pointer when create map.
	ErrValueTypeNotPointer = errors.New("value type must be pointer")
	// ErrExist is return if key is exist when store.
	ErrExist = errors.New("key is already exist")
)
