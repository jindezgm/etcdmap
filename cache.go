/*
 * @Author: jinde.zgm
 * @Date: 2020-09-03 22:42:51
 * @Descripttion:
 */

package etcdmap

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/jindezgm/concurrent"
	"go.etcd.io/etcd/clientv3"
)

// cache implement Map.
type cache struct {
	Codec                       // Value codec
	ctx      context.Context    // Cache context.
	cancel   context.CancelFunc // Cache cancel function.
	client   *clientv3.Client   // ETCD client.
	values   concurrent.Map     // Cached values.
	path     string             // Map path in ETCD.
	typ      reflect.Type       // Value type
	handlers []*handler         // Event handlers.
	futurec  chan future        // Future channel.
	done     chan struct{}      // Cache closed flag.
}

var _ Map = &cache{}

// future is an event that will occur on the cache in the near future.
// It may be faster than the cache synchronization etcd.
// Only values that do not exist in the cache will sent future event to the run coroutine,
// otherwise concurrent.Map.Update() can concurrent update and delete directly.
// Because the run coroutine is responsible for synchronizing etcd,
// creating a new value in the cache may conflict with the run coroutine.
// The update and deletion of the cache is implemented through concurrent.Map.Update(),
// so there is no confict.
type future struct {
	key      string               // Future key
	value    interface{}          // Future value
	revision int64                // Future revision
	result   chan revisionedValue // Result channel of apply future
}

// newCache create cache.
func newCache(client *clientv3.Client, path string, typ reflect.Type, codec Codec, handlers ...EventHandler) (*cache, error) {
	// Use json codec if not specific.
	if nil == codec {
		codec = &jsonCodec{}
	}
	// Create cache.
	c := &cache{
		client:   client,
		path:     path,
		typ:      typ,
		done:     make(chan struct{}),
		handlers: make([]*handler, len(handlers)),
		Codec:    codec,
		futurec:  make(chan future),
	}
	// We need to make sure the key ended with "/" so that we only get children "directiories"
	// e.g. if we have key "/a", "/a/b", "/ab", getting keys with prefix "/a" will return all three,
	// while with prefix "/a/" will return only "/a/b" which is the correct answer.
	if path[len(path)-1] != '/' {
		c.path = path + "/"
	}
	// Initialize cache.
	rev, err := c.sync()
	if nil != err {
		return nil, err
	}
	// Create handlers.
	c.ctx, c.cancel = context.WithCancel(context.Background())
	for i := range handlers {
		c.handlers[i] = newHandler(handlers[i])
	}
	go c.run(rev)

	return c, nil
}

// sync synchronize all nodes under the specified path of ETCD to the map
func (c *cache) sync() (int64, error) {
	key, options := c.path, []clientv3.OpOption{clientv3.WithPrefix()}
	var revision int64
	var lastKey string

	for {
		getResp, err := c.client.KV.Get(c.ctx, key, options...)
		if nil != err {
			return 0, err
		}

		if len(getResp.Kvs) == 0 && getResp.More {
			return 0, fmt.Errorf("no results were fount, but etcd indicated there were more values remaining")
		}

		for _, kv := range getResp.Kvs {
			lastKey = string(kv.Key)
			rv := revisionedValue{value: reflect.New(c.typ).Interface(), revision: kv.ModRevision}
			if err := c.Unmarshal(kv.Value, rv.value); nil == err {
				c.values.Store(string(kv.Key[len(c.path):]), rv)
			}
		}

		if getResp.Header.Revision > revision {
			revision = getResp.Header.Revision
		}

		if !getResp.More {
			break
		}

		key = lastKey + "\x00"
	}

	return revision, nil
}

// Load implement Map.Load()
func (c *cache) Load(key string) (interface{}, bool) {
	// First lookup cache.
	if value, exist := c.values.Load(key); exist {
		return value.(revisionedValue).value, true
	}
	// It should be noted that c.get() returned is not revisionedValue.
	value, exist, _ := c.get(key)
	return value, exist
}

// get value from ETCD and update cache if exist
func (c *cache) get(key string) (interface{}, bool, error) {
	// Get value from ETCD
	getResp, err := c.client.KV.Get(c.ctx, c.path+key)
	if nil != err {
		return nil, false, err
	}
	// Exist?
	if len(getResp.Kvs) == 0 {
		return nil, false, nil
	}
	// Unmarshal value.
	var value interface{}
	if err := unmarshalValue(getResp.Kvs[0].Value, &value, c.typ, c); nil != err {
		return nil, false, err
	}
	// Update into cache
	rv, ok := c.updateIfNewer(key, value, getResp.Kvs[0].ModRevision, false)
	if !ok && rv.value == nil {
		result := make(chan revisionedValue)
		c.futurec <- future{key: key, value: value, revision: getResp.Kvs[0].ModRevision, result: result}
		rv = <-result
	}

	return rv.value, true, nil
}

// updateIfNewer update cached update value if the revision is higher, which is equivalent to newer value
func (c *cache) updateIfNewer(key string, value interface{}, revision int64, updateNotFound bool) (revisionedValue, bool) {
	// Call concurrent.Map.Update() try update
	rv := revisionedValue{value: value, revision: revision}
	ok := c.values.Update(key, func(value interface{}) (interface{}, int) {
		if nil == value { // Update if no value
			if updateNotFound {
				return rv, +1
			}
			rv = revisionedValue{}
			return nil, 0
		} else if cached := value.(revisionedValue); revision > cached.revision { // Update if higher revision
			return rv, +1
		} else { // Get the newer value from cache
			rv = cached
			return nil, 0
		}
	})

	return rv, ok
}

// Store implement Map.Store()
func (c *cache) Store(key string, value interface{}) error {
	// Marshal value
	data, err := c.Marshal(value)
	if nil != err {
		return err
	}
	// Write into ETCD first.
	// Why not check the value exist by the cache? Because cache may be slower than ETCD.
	// The value in the cache may have been deleted in ETCD, the delete event has not arrived yet.
	path := c.path + key
	txnResp, err := c.client.KV.Txn(c.ctx).If(
		clientv3.Compare(clientv3.ModRevision(path), "=", 0),
	).Then(
		clientv3.OpPut(path, string(data)),
	).Commit()
	if nil != err {
		return err
	}
	// Exist?
	if !txnResp.Succeeded {
		return ErrExist
	}
	// Update cache
	result := make(chan revisionedValue)
	c.futurec <- future{
		key:      key,
		value:    value,
		revision: txnResp.Responses[0].GetResponsePut().Header.Revision,
		result:   result,
	}
	<-result

	return nil
}

// Delete implement Map.Delete()
func (c *cache) Delete(key string) error {
	// Only delete the value in the cache if it is successfully deleted from ETCD.
	resp, err := c.client.KV.Delete(c.ctx, c.path+key)
	if nil == err && len(resp.PrevKvs) != 0 {
		c.delete(key, resp.PrevKvs[0].ModRevision)
	}

	return err
}

// delete value by CAS.
func (c *cache) delete(key string, revision int64) {
	c.values.Update(key, func(value interface{}) (interface{}, int) {
		if nil == value {
			return nil, 0
		} else if cached := value.(revisionedValue); revision >= cached.revision {
			return nil, -1
		}
		return nil, 0
	})
}

// Range implement Map.Range()
func (c *cache) Range(f func(key string, value interface{}) bool) {
	c.values.Range(func(key, value interface{}) bool {
		return f(key.(string), value.(revisionedValue).value)
	})
}

// Update implement Map.Update()
func (c *cache) Update(key string, tryUpdate func(value interface{}) (interface{}, error)) error {
	// Optimistically believe that the value in the cache is the latest.
	var rv revisionedValue
	if value, exist := c.values.Load(key); exist {
		rv = value.(revisionedValue)
	}
	// Keep trying to update
	path := c.path + key
	for {
		// Get the updated value
		var err error
		if rv.value, err = tryUpdate(rv.value); nil != err {
			return err
		} else if nil == rv.value {
			return nil
		}
		// Marshal value
		var data []byte
		if data, err = c.Marshal(rv.value); nil != err {
			return err
		}
		// Update the value in ETCD by transaction
		var txnResp *clientv3.TxnResponse
		if txnResp, err := c.client.KV.Txn(c.ctx).If(
			clientv3.Compare(clientv3.ModRevision(path), "=", rv.revision),
		).Then(
			clientv3.OpPut(path, string(data)),
		).Else(
			clientv3.OpGet(path),
		).Commit(); nil != err {
			return err
		} else if txnResp.Succeeded {
			// Get the lastest revision
			rv.revision = txnResp.Responses[0].GetResponsePut().Header.Revision
			break
		}
		// Unmarshal new value and try to update again.
		getResp := (*clientv3.GetResponse)(txnResp.Responses[0].GetResponseRange())
		if len(getResp.Kvs) == 0 {
			rv = revisionedValue{}
		} else if err = unmarshalValue(getResp.Kvs[0].Value, &rv.value, c.typ, c); nil != err {
			return err
		}
		rv.revision = getResp.Kvs[0].ModRevision
	}
	// Update cache.
	c.updateIfNewer(key, rv.value, rv.revision, false)

	return nil
}

// Close implement Map.Close().
func (c *cache) Close() {
	c.cancel()
	<-c.done
}

// run watch ETCD and process events to sync between cache and etcd.
func (c *cache) run(initialRev int64) {
	// Run all event handlers.
	stopc := make(chan struct{})
	for i := range c.handlers {
		go c.handlers[i].run(stopc)
	}
	// Destruct function
	defer func() {
		close(stopc)
		for i := range c.handlers {
			<-c.handlers[i].done
		}
		close(c.done)
	}()
	// If watch ETCD error continues to try
	for rev, err := c.processEvent(initialRev); nil != err; rev, err = c.processEvent(rev) {
		fmt.Println(rev, err)
		time.Sleep(time.Second)
	}
}

// processEvent update the cache based on watched events and dispatch them to event handlers
func (c *cache) processEvent(rev int64) (int64, error) {
	// Watch ETCD event
	wc := c.client.Watch(c.ctx, c.path, clientv3.WithRev(rev+1), clientv3.WithPrevKV(), clientv3.WithPrefix())

	for {
		select {
		// When the Store, Delete, and Update interfaces of the map are called, cache will
		// immediately update the state. At this time, it may conflict with the run coroutine.
		// The way to resolve conflicts is to always apply the maximum revision state of value.
		// Use the CAS capability of concurrent.Map.Update() to solve concurrency problems.
		// But there is a special case, creating a new KV in the cache, because there is no revision
		// of the previous value to compare (such as the value that was just deleted).
		// At this time, we can only turn multi-coroutine concurrency into single-coroutine sequential
		// execution to avoid conflicts, and the new KV of the cache is created by the run coroutine.
		case future := <-c.futurec:
			// Future events may also become obsolete. Only revision of future events larger than
			// the currently synchronized revision are future events.
			rv, _ := c.updateIfNewer(future.key, future.value, future.revision, future.revision > rev)
			future.result <- rv
		// ETCD events.
		case resp, ok := <-wc:
			// Quit?
			if !ok {
				return rev, nil
			}
			// Error?
			if resp.Err() != nil {
				return rev, resp.Err()
			}
			// Iterate events.
			for _, e := range resp.Events {
				// If the previous value is nil, error
				// One example of how this is possible is if the previous value has been compacted already.
				if !e.IsCreate() && e.PrevKv != nil {
					panic(fmt.Errorf("etcd event received with PrevKv=nil (key=%q, modRevision=%d, type=%s)",
						string(e.Kv.Key), e.Kv.ModRevision, e.Type.String()))
				}
				// Parse ETCD'S event to event
				event, err := parseEvent(e, c.path, c.typ, c)
				if nil != err {
					// Failure to parse the event will result in al inconsistent state, what should I do?
					panic(err)
				}
				// Update cache.
				if !event.isDeleted {
					c.updateIfNewer(event.key, event.value, event.rev, true)
				} else {
					c.delete(event.key, event.rev)
				}
				// Update revision to breakpoint watch
				if event.rev > rev {
					rev = event.rev
				}
				// Dispatch event to handler
				for i := range c.handlers {
					c.handlers[i].eventc <- event
				}
			}
		}
	}
}
