<!--
 * @Author: jinde.zgm
 * @Date: 2020-09-03 22:54:55
 * @Descripttion: 
-->
## Introduction
etcdmap mount all nodes undeer the specified path of the tecd to sync.Map, access etcd is as easy as using sync.Map.   
Synchronization map and etcd only need to watch etcd events, and then apply the events to the map. But the consistency is relatively weak, because the recently updated value may not be applied to the map immediately. The general application hopes that the updated value can be strong consistency, because it is likely to be referenced immediately. As for the values ​​updated by others don’t care so much, and the final consistency is enough.   
etcdmap uses the CAS capability of concurrent.Map.Update() to achieve local write/update strong consistency, and the final consistency of remote write/update through watch etcd events.
## Features:
- The key type of the map is fixed to string
- Each path mapping of etcd can only specify one value type
- Write-through mode
- Coroutine safe
- Support update
- Support custom codec