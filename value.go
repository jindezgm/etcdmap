/*
 * @Author: jinde.zgm
 * @Date: 2020-09-03 22:43:43
 * @Descripttion:
 */

package etcdmap

// revisionedValue is revisioned value.
type revisionedValue struct {
	value    interface{}
	revision int64
	dirty    bool
}
