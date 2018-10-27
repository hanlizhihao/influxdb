## 代码结构
* Server代表一个数据库实例，包含Config与多个Service，每个Service是一种服务，分别是：precreation(预创建)、cache snapshot(缓存快照)、snapshot服务、连续查询服务、存储统计服务、Http服务，Server与Service都有Open方法，Open方法负责将开启服务，首先添加服务，然后再逐个开启
* WritePointsRequest是一个写相关的请求
* Server的PointsWriter提供了其他服务向channel写入数据的方法，后经过subscriber分发请求，再由其他线程向各个观察者数据库写入数据
* 底层存储如何提供服务？coordinator 的PointsWriter的方法WritePointsPrivileged
* Database Query 核心在statement_executor，ExecuteStatement将执行一条一条的语句，executeShowMeasurementsStatement负责查询表，store下的MeasurementNames方法将负责查询指标名称
* statement_executor也是集群的基础，读(转发，分发请求后合并结果集)，写(转发-合理的分配series所在节点)
* statement_executor.executeSelectStatement涉及query.SelectOptions(添加了属性，并没有初始化)以及query.ExecutionContext(添加，未初始化)
* tsdb.Engine下的 NewInmemIndex 是真实创建Index的函数，tsdb.engine.index.inmem的meta负责底层创建measurement 和 series
* tsdb.store.shards 下的shard保存着inmem(interface{})类型，实际类型inmem.Index
* Cluster的Series不会因为节点加入或者节点退出而变化 
* httpd handler的路由显示write只能由serveWrite来处理，不是由StatementExecutor来处理，serveWrite的主流程-MapShard查找Shard再由Shard(TSM引擎)来写入数据(与此同时，向subscriber分发请求)
* Series可能在多个Shard上存在，Shard表示一段时间范围的数据(所有Database)，Store、Shard、Index均保存相同的*tsdb.SeriesFile，从而共享Series
### 设计
```
tsdb-cluster-auto-increment-id
tsdb-node-auto-increment-id  
tsdb-common-node                {ID:uint64,Host:string,TCPHost:string}             
tsdb-available-clusters         所有集群，value是json,[{id:1, nodes:[{id:1,host:,udpHost:}]}]
tsdb-recruit-clusters           正在招募节点value,"{number:0, clusterIds:["1"]}"
tsdb-work-cluster-{id}          可以提供服务的集群key,按照id获取，value是{limit:1,number, nodes:[{id:1,host:192.168.119}], series:[{key:hashkey,size}]}
tsdb-work-cluster-lock-{id}     值是特定cluster的节点的数量
tsdb-recruit-cluster-{id}       正在招募的集群value是集群信息：{number:1,limit:1, nodes:[{id:,host:,udpHost:}]}
tsdb-databases                  {database: [{name: string, rp: {name: string, replica: *int, duration: *time.Duration, shardGroupDuration: time.Duration}}]}
tsdb-continuous-queries          [{name:, series. clusterId:}]           

节点加入以后注意判断是否为可用
watch database变化
```
## 隐藏问题
* Database and retention policy data are consistent 来自元数据，由于网络通信延迟可能出现创建的数据库与保留策略尚不一致，已经接受到写入新数据库的请求
