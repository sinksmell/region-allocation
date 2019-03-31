### 分布式系统中3副本2DC放置策略

[![GoDoc](https://godoc.org/github.com/sinksmell/region-allocation?status.svg)](https://godoc.org/github.com/sinksmell/region-allocation)

[![Go Report Card](https://goreportcard.com/badge/github.com/sinksmell/region-allocation)](https://goreportcard.com/report/github.com/sinksmell/region-allocation)


**核心算法**

* 1. 将一个副本存放在本地Rack的任意节点(如果没有指定本地Rack，则随机选一个) 
* 2. 将一个副本放在与1相同DC不同Rock的节点上
* 3. 将最后一个副本放置在与1，2不同DC的任意节点上


#### DC 、Rock 、Store节点的分布图

![](https://i.loli.net/2019/03/27/5c9b39218dff1.png)


#### 3副本检查及再分配策略

* 流程图

![](https://i.loli.net/2019/03/29/5c9e0b7e22de2.png)