package main

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"time"
)

// 从配置文件读取store分配情况
type MockJson struct {
	Dcs []DC `json:"dcs"`
}

// 数据中心 DC 与 Rock 为一对多关系
type DC struct {
	ID    int     `json:"id"`
	Name  string  `json:"name"`
	Rocks [] Rock `json:"rocks"`
}

// Rock 与DC为多对一
// Rock机架 Rock 与 Store为多对一关系
type Rock struct {
	ID     int     `json:"id"`
	Name   string  `json:"name"`
	Dc     *DC     `json:"-"`
	Stores []Store `json:"stores"`
}

// 存储节点
type Store struct {
	ID             int      `json:"id"`
	LocationLabels []string `json:"labels"`
	Rock           *Rock    `json:"-"`
}

// 分片
type Region struct {
	Replicas []int
}

//
type Strategy struct {
	//TODO
	Dcs []DC
}

// 检查是否满足策略约束 不满足就重新分配
func Check(stores []Store, region Region, strategy Strategy) Region {

	// region还没有被分配 就尝试进行分配
	if len(region.Replicas) == 0 {
		//	return
		if reg,err:=strategy.TryAllocate(stores);err!=nil{
			// 如果分配出错 则返回原来的 region
			return region
		}else{
			// 分配结果重新
			return *reg
		}
	}

	//TODO
	return Region{}
}

func main() {
	MockData()
	//ProducMock()
}




func (stgy *Strategy) TryAllocate(stores []Store) (region *Region, err error) {

	var (
		lens  int
		index int
		store Store
		rock  *Rock
		dc    *DC
		rd    *rand.Rand
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
	store = stores[index]
	// 存储到对应的节点
	region.Replicas = append(region.Replicas, store.ID)
	// 保存当前节点对应的Rock
	rock = store.Rock
	// 保存当前节点对应的DC
	dc = rock.Dc
	lens = len(dc.Rocks)
	// 2.一个副本放在本地DC的不同Rock上
	// 选择一个与当前节点在相同DC不同Rock的节点
	for {
		rockI := rd.Intn(lens)
		rock2 := dc.Rocks[rockI]
		if (rock2.ID != rock.ID) {
			storeI := rd.Intn(len(rock2.Stores))
			store = rock2.Stores[storeI]
			region.Replicas = append(region.Replicas, store.ID)
			break
		}
	}

	// 3.最后一个副本放在不同DC的任意节点上
	lens = len(stores)
	for {
		storeI := rd.Intn(lens)
		store = stores[storeI]
		if store.Rock.Dc.ID != dc.ID {
			region.Replicas = append(region.Replicas, store.ID)
			break
		}
	}

	return
}



func ProducMock() {

	Dc1 := DC{
		ID:    1,
		Name:  "shanghai",
		Rocks: make([]Rock, 0),
	}

	Dc2 := DC{
		ID:    1,
		Name:  "shanghai",
		Rocks: make([]Rock, 0),
	}

	for i := 0; i < 10; i++ {
		rock := Rock{
			ID:     i,
			Name:   fmt.Sprintf("Rock%02d", i),
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
				rock.Stores[k].Rock = &rock
			}
		}
	}

	//for _,dc:=range mockData.Dcs {
	//	fmt.Printf("%02d %s RocksId: \n",dc.ID,dc.Name)
	//	for _,rock:=range dc.Rocks {
	//		fmt.Printf("\t %d %s   Stores: \n",rock.ID,rock.Name)
	//		for _,store:=range rock.Stores  {
	//			fmt.Printf("\t\t StoreId: %02d  belongto: RockID %02d\n",store.ID,store.Rock.ID)
	//		}
	//	}
	//	fmt.Println("--------")
	//}

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
				fmt.Printf("\t\t StoreId: %02d  belongto: RockID %02d\n", store.ID, store.Rock.ID)
			}
		}
		fmt.Println("--------")
	}
}