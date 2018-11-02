# big-data

## 项目计划
整合各大数据框架的example工程，通过对example代码的编写和理解达到在学习的目的。
计划将包含一下框架的示例代码：
- curator
- spark
- hadoop
- kafka
- zookeeper(zkclient)

## 项目完成度
1. curator
  - async curator异步通信，采用java8 CompleteFuture模式实现
  - cache curator缓存实现
  - discovery 服务发现功能
  - framework curator基本常用功能，`get`, `put`, `create`等
  - leader 选主实现
  - locking 分布式锁实现
  - modeled curator基于异步通信框架实现的模式化操作zookeeper功能，简化代码复杂度
  - pubsub 基于modeled实现的发布订阅功能
