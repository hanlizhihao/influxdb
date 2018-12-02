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
* shard.validateSeriesAndFields调用engine.createSeriesListIfNotExist->index.createSeriesListIfNotExist->partition.createExist->log_file
### 设计
```
tsdb-cluster-auto-increment-id
tsdb-node-auto-increment-id  
tsdb-class-auto-increment-id    
tsdb-common-node                {ID:uint64,Host:string,TCPHost:string}             
tsdb-available-clusters         可用集群，value是json,[{id:1, nodes:[{id:1,host:,udpHost:}]}]
tsdb-work-cluster-{id}          可以提供服务的集群key,按照id获取，value是{limit:1,number, nodes:[{id:1,host:192.168.119}], series:[{key:hashkey,size}]} 
tsdb-databases                  {database: [{name: string, rp: {name: string, replica: *int, duration: *time.Duration, shardGroupDuration: time.Duration}}]}
tsdb-continuous-queries          [{name:, series. clusterId:}]           
特别注意，classes设计有增量更新标识，newMeasurement表示每次更新classes时，新增的classes，deleteMeasurement表示每次更新classes时，删除的classes，
特别注意，在每次实际更新measurment时，再修改上次保存的measurement和deleteMeasurement
tsdb-classes-info                 [{classId, limit, clusterIds:[1,2,3]}]  1有无该节点，2有该节点，遍历数组，尝试将clusterId加入
tsdb-class-id                     {clusters:[{id,masterNode:{id,host,weight}}], measurements: [name]}

node->cluster->class  节点组成cluster，cluster组成class，每个class将负责多个表的全部数据
1.快速判断condition是否命中外部Series
tagKey检索通过map索引实现，tagValue中检索Value通过b+树索引
1.1简单实现：一个表下的series进行分片，500个series以内单组，500至1000两组，1000到6000三组，6000以上全组
在handler层，只需要判断查询是否命中本组的表，不是本组表，则balance负载均衡，是本组表结果集合并处理流程
写：
    特别说明：班级下的表是共享的，班级下的所有组都存储着相同的多个表，但是series完全不同。cluster不允许存储表的信息了，
    加入新班级时，无论cluster原来是否保存了表，都不用管，只需要认为，它具有了本班的所有表
    (1)write point首先判断是不是local class包含的表，是，则一致性Hash，不是，则forward，不改变请求头的情况下
    (2)一致性Hash以后，将写到相同小组的point缓存，批量写到班级下的其他小组中，写入其他小组的请求头是带有特定标识的，
    负载均衡器识别特定请求头后，将不做分析，直接写入本地硬盘存储。
    (3)对于分发写入请求的节点，等待所有被分发写请求完成以后，删除内存数据，写入失败的，写入本地磁盘，等待重试，
    两次以上写入失败的写入磁盘，注意，写请求，如果不负载均衡的情况下，立即响应写入完成，如果负载均衡，等待转发后的响应结果
    
2.1分布式索引是指，在小组内均衡索引数据，使用每个节点的一定内存来存储索引，通过简单的Http接口小组互相访问索引，以解决内存占用过大，保存在磁盘上响应速度较慢的问题
2.2分布式索引的均衡可以通过一致性Hash算法来解决，整体的实现方式类似memached
3.1核心读合并结果集的实现是全组转发，Single Cluster Booster 直接基于time进行分配，将有效加速BigSql查询，提高数倍磁盘IO性能
```
## 隐藏问题
* master 节点挂掉，暂时没有选举功能
* DML没有集群化
* metaData如何保存在磁盘上的
* 先创建RecruitCluster，在节点数量大于2后，创建worker，在转变可用集群以后，加入class
* 对于新的表名，需要向class注册
* classIpMap，当ip数组数量小于等于1，重新build，使用map索引时，出现失效，则删除数组元素 - 待验证
* 属于本地class，但series不属于本地的数据处理失败，暂时未作处理，只是重试3次
* 异步EtcdSerivce写入本地失败，暂时未处理
*