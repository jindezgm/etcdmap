/*
 * @Author: jinde.zgm
 * @Date: 2020-09-05 15:38:48
 * @Descripttion:
 */

package etcdmap

import "encoding/json"

// Codec defines the interfaces of encode/decode value
type Codec interface {
	Marshal(value interface{}) ([]byte, error)
	Unmarshal(data []byte, value interface{}) error
}

// jsonCodec use json.Marshal/Unmarshal encode/decode value.
type jsonCodec struct{}

// Marsahl implement Codec.Marshal()
func (c *jsonCodec) Marshal(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

// Unmarshal implement Codec.Unmarshal()
func (c *jsonCodec) Unmarshal(data []byte, value interface{}) error {
	return json.Unmarshal(data, value)
}
