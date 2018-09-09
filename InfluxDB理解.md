## 代码结构
* Server代表一个数据库实例，包含Config与多个Service，每个Service是一种服务，分别是：precreation(预创建)、cache snapshot(缓存快照)、snapshot服务、连续查询服务、存储统计服务、Http服务，Server与Service都有Open方法，Open方法负责将开启服务，首先添加服务，然后再逐个开启
