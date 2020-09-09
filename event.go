/*
 * @Author: jinde.zgm
 * @Date: 2020-09-03 22:43:10
 * @Descripttion:
 */

package etcdmap

import (
	"fmt"
	"reflect"

	"go.etcd.io/etcd/clientv3"
)

// event record the state of event that happen to a value.
type event struct {
	key       string      // Value's key
	value     interface{} // The value for the event.
	preValue  interface{} // The value before the event happens.
	rev       int64       // Revision of the last modification on the key.
	isDeleted bool        // Delete event or not.
	isCreated bool        // Create event or not.
}

// parseEvent convert etcd's event ot event.
func parseEvent(e *clientv3.Event, prefix string, typ reflect.Type, codec Codec) (*event, error) {
	// If the previous value is nil, error.
	// On example of how this is possible is if the previous value has been compated already.
	if !e.IsCreate() && e.PrevKv == nil {
		return nil, fmt.Errorf("etcd evetn received with PreKv=nil (key=%q, modRevision=%d, type=%s)",
			string(e.Kv.Key), e.Kv.ModRevision, e.Type.String())
	}
	// Create event.
	ret := &event{
		key:       string(e.Kv.Key[len(prefix):]),
		rev:       e.Kv.ModRevision,
		isDeleted: e.Type == clientv3.EventTypeDelete,
		isCreated: e.IsCreate(),
	}
	// Only the delete event without value.
	if !ret.isDeleted {
		if err := unmarshalValue(e.Kv.Value, &ret.value, typ, codec); nil != err {
			return nil, err
		}
	}
	// Decode previous value.
	if nil != e.PrevKv && len(e.PrevKv.Value) > 0 {
		if err := unmarshalValue(e.PrevKv.Value, &ret.preValue, typ, codec); nil != err {
			return nil, err
		}
	}

	return ret, nil
}
