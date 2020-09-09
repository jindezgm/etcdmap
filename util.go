/*
 * @Author: jinde.zgm
 * @Date: 2020-09-03 22:43:31
 * @Descripttion:
 */

package etcdmap

import "reflect"

// unmarshalValue ummarshal value.
func unmarshalValue(data []byte, value *interface{}, typ reflect.Type, codec Codec) error {
	*value = reflect.New(typ).Interface()
	return codec.Unmarshal(data, *value)
}
