## 代码结构
* Server代表一个数据库实例，包含Config与多个Service，每个Service是一种服务，分别是：precreation(预创建)、cache snapshot(缓存快照)、snapshot服务、连续查询服务、存储统计服务、Http服务，
Server与Service都有Open方法，Open方法负责将开启服务，首先添加服务，然后再逐个开启
* WritePointsRequest是一个写相关的请求
* Server的PointsWriter提供了其他服务向channel写入数据的方法，后经过subscriber分发请求，再由其他线程向各个观察者数据库写入数据
* 底层存储如何提供服务？coordinator 的PointsWriter的方法WritePointsPrivileged
* Database Query 核心在statement_executor，ExecuteStatement将执行一条一条的语句，executeShowMeasurementsStatement负责查询表，store下的MeasurementNames方法将负责查询指标名称
* SelectStatement执行过程，通过Executor的ExecuteQuery得到Result的通道，Result简单表示为一行数据，对于通道数据的处理有异步和同步两种方式，statement首先经过预编译，添加默认查询参数，
再底层由query下的select执行查询返回Cursor(游标)，Emitter发射器将处理Cursor中的结果，如果返回数据过大，还会进行截断，Emitter将通过ExecutionContext将数据发送给handler中的处理函数
influxql.SelectStatement的Fields属性包含的是数据的tag部分以及value部分，Sources部分描述查询的目标数据库（以及涉及数据库指标（数据库名称、RP、指标名称、是否是Target）），
Condition描述一系列Select筛选条件，每个筛选条件是一个BinaryExpr(Op:等于、大于、小于、LHS：条件的左表达式、RHS条件的右表达式)
Emitter的Emit方法中cursor.Scan方法将得到值，Cursor为数据的来源，注意：Cursor的scan方法通过调用至query\iterator.gen.go中的FloatIterator(或者其他的Iterator)的Next()方法调用至tsm1\floatIterator 结构体
这个结构体同样实现了在query\iterator.gen.go文件下query\FloatIterator接口
查询每行数据的核心是Cursor的Scan方法，Scan方法初始调用的scan方法，就是cursor下的scan方法
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
tsdb-available-clusters         可用集群，value是json,[{id:1, nodes:[{id:1,host:,udpHost:}]}]
tsdb-recruit-clusters           正在招募节点value,"{number:0, clusterIds:["1"]}"
tsdb-work-cluster-{id}          可以提供服务的集群key,按照id获取，value是{limit:1,number, nodes:[{id:1,host:192.168.119}], series:[{key:hashkey,size}]} 
tsdb-recruit-cluster-{id}       正在招募的集群value是集群信息：{number:1,limit:1, nodes:[{id:,host:,udpHost:}]}
tsdb-databases                  {database: [{name: string, rp: {name: string, replica: *int, duration: *time.Duration, shardGroupDuration: time.Duration}}]}
tsdb-continuous-queries          [{name:, series. clusterId:}]           

快速判断condition是否命中外部Series
tagKey检索通过map索引实现，tagValue中检索Value通过b+树索引
简单实现：一个表下的series进行分片，500个series以内单组，500至1000两组，1000到6000三组，6000以上全组
在handler层，只需要判断查询是否命中本组的表，不是本组表，则balance负载均衡，是本组表结果集合并处理流程
```
## 隐藏问题
* Data consistency is achieved through distributed locks, which applies the database, retention policy and series
* master 节点挂掉，暂时没有选举功能
* DML没有集群化
* 负载均衡测试
