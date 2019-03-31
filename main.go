package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"time"
)

// 从配置文件读取store分配情况
type MockJson struct {
	Dcs []DC `json:"dcs"`
}

// 数据中心 DC 与 Rack 为一对多关系
type DC struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Rocks []Rack `json:"racks"`
}

// Rack 与DC为多对一
// Rack机架 Rack 与 Store为多对一关系
type Rack struct {
	ID     int     `json:"id"`
	Name   string  `json:"name"`
	Dc     *DC     `json:"-"`
	Stores []Store `json:"stores"`
}

// 存储节点
type Store struct {
	ID             int      `json:"id"`
	LocationLabels []string `json:"labels"`
	Rack           *Rack    `json:"-"`
}

// 分片
type Region struct {
	Replicas []int
}

// 策略调整器
type Strategy struct {
	NodeMap map[int]Store // 为了方便地根据StoreId 查询对应的Store对象
}

func main() {

	var (
		stores []Store    // 存放所有可用节点
		data   *MockJson  // json文件对应的数据
		stgy   *Strategy  // 策略调整器
		region Region     // 副本分布结果
		rd     *rand.Rand // 随机数发生器
		arr    []int      // 存放随机生成的副本分布
		err    error
	)
	// 1.初始化测试数据
	// 初始化 随机数发生器
	rd = rand.New(rand.NewSource(time.Now().UnixNano()))
	arr = make([]int, 3)

	// 初始化DC Rack Store分配数据
	if data, err = MockData(); err != nil {
		goto ERR
	}
	// 初始化策略对象
	stgy = &Strategy{NodeMap: make(map[int]Store)}
	// 初始化节点集合
	stores = make([]Store, 0)
	for i := range data.Dcs {
		dc := data.Dcs[i]
		for j := range dc.Rocks {
			rock := dc.Rocks[j]
			for k := range rock.Stores {
				store := rock.Stores[k]
				stores = append(stores, store)
				stgy.NodeMap[store.ID] = store
			}
		}
	}

	// 打印系统节点分布图
	fmt.Println("Store分布:")
	PrintStores(data)

	// 2. 从0(副本个数为0)开始进行副本分配
	region = Check(stores, region, *stgy)
	fmt.Println("从0开始分配: ", region)
	stgy.PrintRegion(&region)
	fmt.Println("")
	// 3.随机生成一个副本分配情况进行检查
	for i := 0; i < len(arr); i++ {
		arr[i] = rd.Intn(len(stores))
	}

	region = Region{Replicas: arr}
	fmt.Println("随机生成的待检测副本分布: ", region)
	stgy.PrintRegion(&region)
	region = Check(stores, region, *stgy)
	fmt.Println("调整后的副本分布: ", region)
	stgy.PrintRegion(&region)
	return

ERR:
	fmt.Println(err)

}

// 检查是否满足策略约束 不满足就重新分配
func Check(stores []Store, region Region, strategy Strategy) Region {

	var (
		//nums           = 3   // 节点数量
		nodes     []int // 去重后的节点集合
		nodes2    []int // 去除重复Rack之后的节点集合
		localNode Store
		//localRack *Rack
		err error
	)
	nodes = make([]int, 0)
	nodes2 = make([]int, 0)
	// 1. region还没有被分配 就尝试进行分配
	if region.Replicas == nil || len(region.Replicas) == 0 {
		//	return
		reg := strategy.Allocate(stores)
		return *reg
	}
	// 2. region中存在节点重复 对节点进行去重
	nodes = strategy.RemoveReptElem(region.Replicas)

	// 3.节点不重复 但rack重复 对rack去重
	nodes2 = strategy.RemoveReptRack(nodes)

	// 打印 去重之后的信息 查看是否正确去重
	/*	fmt.Println("Store和Rack去重后: ", nodes2)
		for _, value := range nodes2 {
			node, _ := strategy.FindNode(value)
			fmt.Println(node.Rack.ID)
		}*/

	// 选取第一个节点作为本地节点
	if localNode, err = strategy.FindNode(nodes2[0]); err != nil {
		// 没找到对应的节点则直接返回
		goto ERR
	}

	// 4. 此时Store和Rack完成了去重
	// 存留的元素可能有 1 2 3
	switch len(nodes2) {
	case 1:
		reg := strategy.ReAllocate1(stores, &localNode)
		region = *reg
	case 2:
		reg := strategy.ReAllocate2(stores, nodes2)
		region = *reg
	case 3:
		reg := strategy.ReAllocate3(stores, nodes2)
		region = *reg
	}

	return region

ERR:
	fmt.Println(err)
	return region
}

func (stgy *Strategy) Allocate(stores []Store) (region *Region) {

	var (
		lens           int
		index          int
		localNode      Store
		node           Store
		localRack      *Rack
		localDc        *DC
		rd             *rand.Rand
		otherRackNodes []Store
		otherDcNodes   []Store
	)

	// 初始化随机数生成器
	rd = rand.New(rand.NewSource(time.Now().Unix()))

	// 获取节点数量
	lens = len(stores)
	if lens < 3 {
		return
	}
	// 1.将一个副本放在本地DC的本地Rock上  如果不确定本地Rock则随机分配一个
	index = rd.Intn(lens)
	region = &Region{
		Replicas: make([]int, 0),
	}
	localNode = stores[index]
	// 存储到对应的节点
	region.Replicas = append(region.Replicas, localNode.ID)
	// 保存当前节点对应的Rock
	localRack = localNode.Rack
	// 保存当前节点对应的DC
	localDc = localRack.Dc
	// 当期DC不同Rack下的节点
	otherRackNodes = getORackNodes(stores, localRack)
	// 不同DC下的节点
	otherDcNodes = getODcNodes(stores, localDc)

	// 2.一个副本放在本地DC的不同Rock上
	// 选择一个与当前节点在相同DC不同Rock的节点
	node = RandNode(otherRackNodes)
	region.Replicas = append(region.Replicas, node.ID)

	// 3.最后一个副本放在不同DC的任意节点上
	node = RandNode(otherDcNodes)
	region.Replicas = append(region.Replicas, node.ID)

	return
}

// 在只有一个副本的基础上再次分配
func (stgy *Strategy) ReAllocate1(stores []Store, localNode *Store) (region *Region) {
	region = &Region{
		Replicas: make([]int, 0),
	}
	region.Replicas = append(region.Replicas, localNode.ID)
	localRack := localNode.Rack
	localDc := localRack.Dc

	// 同一DC不同Rack的节点的集合
	rackNodes := getORackNodes(stores, localRack)
	// 不同DC的节点集合
	dcNodes := getODcNodes(stores, localDc)
	// 放置第二个副本
	node := RandNode(rackNodes)
	region.Replicas = append(region.Replicas, node.ID)
	// 放置第三个副本
	node = RandNode(dcNodes)
	region.Replicas = append(region.Replicas, node.ID)

	return
}

// 在有两个副本的情况下再次分配
func (stgy *Strategy) ReAllocate2(stores []Store, nodes []int) (region *Region) {

	var (
		localNode      Store
		localRack      *Rack
		localDc        *DC
		otherRackNodes []Store
		otherDcNodes   []Store
		node           Store
	)

	// 本地节点
	localNode, _ = stgy.FindNode(nodes[0])
	localRack = localNode.Rack
	localDc = localRack.Dc
	// 另外一个节点
	otherNode, _ := stgy.FindNode(nodes[1])

	// 初始化本地Dc不同Rack的节点
	otherRackNodes = getORackNodes(stores, localRack)
	// 初始化其他Dc上的节点
	otherDcNodes = getODcNodes(stores, localDc)

	if otherNode.Rack.Dc.ID == localDc.ID {
		// 两个节点在同一DC下
		if otherNode.Rack.ID == localRack.ID {
			// 两个节点在同一Rack下
			// 选择一个同一DC不同Rack的节点
			node = RandNode(otherRackNodes)
			nodes[1] = node.ID

			// 选择一个不同DC的节点
			node = RandNode(otherDcNodes)
			nodes = append(nodes, node.ID)
		} else {
			// 两个节点在相同DC不同Rack上
			// 从另外一个DC选一个节点
			node = RandNode(otherDcNodes)
			nodes = append(nodes, node.ID)
		}

	} else {
		// 两个节点在不同DC下
		// 在本地DC的不同Rack下选择一个节点添加进去即可
		node = RandNode(otherRackNodes)
		nodes = append(nodes, node.ID)
	}

	return &Region{Replicas: nodes}
}

// 3个节点的情况下进行再次分配
func (stgy *Strategy) ReAllocate3(stores []Store, nodes []int) (region *Region) {
	var (
		localNode Store
		localRack *Rack
		localDc   *DC
		cnt       int
		rd        *rand.Rand
	)

	rd = rand.New(rand.NewSource(time.Now().Unix()))

	// 此时Rack已经不重复了
	// 如果分布在两个DC那么此时已经满足了,不需要再次分配了
	// 本地节点
	localNode, _ = stgy.FindNode(nodes[0])
	localRack = localNode.Rack
	localDc = localRack.Dc
	cnt = 1
	for _, id := range nodes {
		node, _ := stgy.FindNode(id)
		if node.Rack.Dc.ID != localDc.ID {
			cnt++
			break
		}
	}

	if cnt > 1 {
		region = &Region{Replicas: nodes}
		return
	}

	// 3节点Rack不重复 但是都在同一个Rack下
	// 随机删除一个节点 重新分配
	delId := rd.Intn(len(nodes))
	nodes2 := make([]int, 0)
	for key, value := range nodes {
		if key == delId {
			continue
		}
		nodes2 = append(nodes2, value)
	}

	// 转化成两节点分配问题
	region = stgy.ReAllocate2(stores, nodes2)
	return
}

// 去重函数,去除重复的节点id,对于重复的元素只保留一个
func (stgy *Strategy) RemoveReptElem(arr []int) (newArr []int) {
	newArr = make([]int, 0)
	for i := 0; i < len(arr); i++ {
		repeat := false
		for j := i + 1; j < len(arr); j++ {
			if arr[i] == arr[j] {
				repeat = true
				break
			}
		}
		if !repeat {
			newArr = append(newArr, arr[i])
		}
	}
	return
}

//	根据Rack来进行去重 有Rack重复则只保留一个节点
func (stgy *Strategy) RemoveReptRack(nodes []int) (res []int) {
	rackBook := make(map[int]bool) // 对rack id标记 根据rack对节点进行去重
	res = make([]int, 0)
	// 遍历 store 根据rack进行去重
	for _, nodeId := range nodes {
		// 根据节点id 来查询节点
		node, err := stgy.FindNode(nodeId)
		if err != nil {
			// 遇到错误说明节点可能不存在 则移除该节点
			continue
		}
		// 遇到已经访问过的Rack上的节点则跳过
		if _, ok := rackBook[node.Rack.ID]; ok {
			continue
		}
		// 加入节点 标记rack
		rackBook[node.Rack.ID] = true
		res = append(res, node.ID)
	}
	return
}

func (stgy *Strategy) FindNode(id int) (node Store, err error) {
	if node, ok := stgy.NodeMap[id]; ok {
		return node, nil
	}
	return node, errors.New("Node not found!")
}

// 获取与本地节点相同DC不同Rack的节点的集合
func getORackNodes(stores []Store, rack *Rack) []Store {
	nodes := make([]Store, 0)
	for i, n := 0, len(stores); i < n; i++ {
		if stores[i].Rack.ID != rack.ID && stores[i].Rack.Dc.ID == rack.Dc.ID {
			nodes = append(nodes, stores[i])
		}
	}
	return nodes
}

// 打印节点分布情况
func (stgy *Strategy) PrintRegion(region *Region) {
	for _, id := range region.Replicas {
		node, _ := stgy.FindNode(id)
		fmt.Printf("StoreId :%2d\tRackId :%2d\tDcId :%2d\n", node.ID, node.Rack.ID, node.Rack.Dc.ID)
	}
}

// 获取与本地节点不同DC的节点的集合
func getODcNodes(stores []Store, dc *DC) []Store {
	nodes := make([]Store, 0)
	for i, n := 0, len(stores); i < n; i++ {
		if stores[i].Rack.Dc.ID != dc.ID {
			nodes = append(nodes, stores[i])
		}
	}
	return nodes
}

// 在已有的stores集合中随机选取一个节点
func RandNode(stores []Store) Store {
	rd := rand.New(rand.NewSource(time.Now().Unix()))
	lens := len(stores)
	i := rd.Intn(lens)
	return stores[i]
}

func MockData() (*MockJson, error) {

	var (
		bytes    []byte
		mockData MockJson
		err      error
	)

	// 从配置文件中读取机房分布
	if bytes, err = ioutil.ReadFile("mock.json"); err != nil {
		//fmt.Println(err)
		return nil, err
	}
	// 解析到模拟数据的结构体中
	if err = json.Unmarshal(bytes, &mockData); err != nil {
		//fmt.Println(err)
		return nil, err
	}
	//fmt.Println(mockData)

	// 建立对象之间的关系
	for index := range mockData.Dcs {
		dc := mockData.Dcs[index]
		for j := range dc.Rocks {
			// dc相同但是dc的内存位置不相同
			dc.Rocks[j].Dc = &dc
			rock := dc.Rocks[j]
			for k := range rock.Stores {
				rock.Stores[k].Rack = &rock
			}
		}
	}

	// 添加节点
	//mockData.Dcs[0].Rocks[0].Stores[0]
	return &mockData, nil
}

// 打印各个节点信息
func PrintStores(data *MockJson) {
	for _, dc := range data.Dcs {
		fmt.Printf("%02d %s RocksId: \n", dc.ID, dc.Name)
		for _, rock := range dc.Rocks {
			fmt.Printf("\t %d %s   Stores: \n", rock.ID, rock.Name)
			for _, store := range rock.Stores {
				fmt.Printf("\t\t StoreId: %02d  belongto: RockID %02d\n", store.ID, store.Rack.ID)
			}
		}
		fmt.Println(">-------------------------------------------------<")
	}
}
