package main

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"time"
	"errors"
)

// 从配置文件读取store分配情况
type MockJson struct {
	Dcs []DC `json:"dcs"`
}

// 数据中心 DC 与 Rack 为一对多关系
type DC struct {
	ID    int     `json:"id"`
	Name  string  `json:"name"`
	Rocks [] Rack `json:"racks"`
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

//
type Strategy struct {
	//TODO
	//Dcs     []DC
	NodeMap map[int]Store // 为了方便地根据StoreId 查询对应的Store对象
}

// 检查是否满足策略约束 不满足就重新分配
func Check(stores []Store, region Region, strategy Strategy) Region {

	var (
		//nums           = 3   // 节点数量
		repeat    bool  // 判断节点是否重复
		nodes     []int // 去重后的节点集合
		nodes2    []int // 去除重复Rack之后的节点集合
		localNode Store
		//node           Store
		localRack *Rack
		//localDc        *DC
		//otherRackNodes []Store // 相同DC 不同Rack的节点
		//otherDcNodes   []Store // 不同DC上的节点
		err error
	)
	nodes = make([]int, 0)
	nodes2 = make([]int, 0)
	//otherRackNodes = make([]Store, 0)
	//otherDcNodes = make([]Store, 0)

	// 1. region还没有被分配 就尝试进行分配
	if region.Replicas == nil || len(region.Replicas) == 0 {
		//	return
		reg := strategy.Allocate(stores)
		return *reg
	}

	repeat = repeat
	// 2. region中存在节点重复 对节点进行去重
	nodes = RemoveReptElem(region.Replicas)

	//fmt.Println("Node",nodes)
	// 3.节点不重复 但rack重复 对rack去重
	// 选取第一个节点作为本地节点
	if localNode, err = strategy.FindNode(nodes[0]); err != nil {
		// 没找到对应的节点则直接返回
		goto ERR
	}
	// 本地Rack
	localRack = localNode.Rack
	// 遍历 store 根据rack进行去重
	nodes2 = append(nodes2, localNode.ID)
	for _, nodeId := range nodes {
		node, err := strategy.FindNode(nodeId)
		if err != nil {
			continue
		}
		// 将不在同一个Rack的节点加入
		if node.Rack.ID != localRack.ID {
			nodes2 = append(nodes2, node.ID)
		}
	}
	// 此时Rack完成了去重
	// 存留的元素可能有 1 2 3
	//fmt.Println("Store和Rack去重后: ", nodes2)
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

func main() {

	// 1.初始化测试数据

	// 2.初始化策略对象

	// 3.从0进行3副本分配

	// 4.随机生成一个副本分配情况进行检查


	MockData()
	//ProducMock()
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
	otherRackNodes = OtherRackNodes(stores, localRack)
	// 不同DC下的节点
	otherDcNodes = OtherDCNodes(stores, localDc)

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
	rackNodes := OtherRackNodes(stores, localRack)
	// 不同DC的节点集合
	dcNodes := OtherDCNodes(stores, localDc)
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
	otherRackNodes = OtherRackNodes(stores, localRack)
	// 初始化其他Dc上的节点
	otherDcNodes = OtherDCNodes(stores, localDc)

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
	delId:=rd.Intn(len(nodes))
	nodes2:=make([]int,0)
	for key, value := range nodes {
		if key==delId{
			continue
		}
		nodes2=append(nodes2,value)
	}

	// 转化成两节点分配问题
	region=stgy.ReAllocate2(stores,nodes2)
	return
}

// 数组去重函数,对于重复的元素只保留一个
func RemoveReptElem(arr []int) (newArr []int) {
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

func (stgy *Strategy) FindNode(id int) (node Store, err error) {
	if node, ok := stgy.NodeMap[id]; ok {
		return node, nil
	}
	return node, errors.New("Node not found!")
}

// 根据Store的id在stores查找节点
func FindNode(stores []Store, id int) (Store, error) {
	for _, node := range stores {
		if node.ID == id {
			return node, nil
		}
	}
	return Store{ID: -1}, errors.New("Can not find this node!")
}

// 获取与本地节点相同DC不同Rack的节点的集合
func OtherRackNodes(stores []Store, rack *Rack) []Store {
	nodes := make([]Store, 0)
	for i, n := 0, len(stores); i < n; i++ {
		if stores[i].Rack.ID != rack.ID && stores[i].Rack.Dc.ID == rack.Dc.ID {
			nodes = append(nodes, stores[i])
		}
	}
	return nodes
}

// 获取与本地节点不同DC的节点的集合
func OtherDCNodes(stores []Store, dc *DC) []Store {
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

func ProducMock() {

	Dc1 := DC{
		ID:    1,
		Name:  "shanghai",
		Rocks: make([]Rack, 0),
	}

	Dc2 := DC{
		ID:    1,
		Name:  "shanghai",
		Rocks: make([]Rack, 0),
	}

	for i := 0; i < 10; i++ {
		rock := Rack{
			ID:     i,
			Name:   fmt.Sprintf("Rack%02d", i),
			Stores: make([]Store, 0),
		}
		store := Store{ID: i, LocationLabels: []string{"hello", "world"}}
		dcId := i % 2
		if dcId == 0 {
			rock.Stores = append(rock.Stores, store)
			rock.Dc = &Dc1
			Dc1.Rocks = append(Dc1.Rocks, rock)
		} else {
			rock.Stores = append(rock.Stores, store)
			rock.Dc = &Dc2
			Dc2.Rocks = append(Dc2.Rocks, rock)
		}
	}

	//for _,v:=range Dc1.Rocks {
	//	fmt.Println(v)
	//}

	bytes, err := json.Marshal(Dc1)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(bytes))
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
