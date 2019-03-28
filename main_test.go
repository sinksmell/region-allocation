package main

import (
	"testing"
	"fmt"
)

// 为了模拟数据方便 初始化几个单例
var (
	strategy *Strategy
	mockData *MockJson
	stores   []Store
)

func Init() {
	var (
		err error
	)
	// 获取模拟数据
	if mockData, err = MockData(); err != nil {
		fmt.Println(err)
		return
	}

	// 初始化 strategy
	strategy = &Strategy{
		Dcs: make([]DC, 0),
		NodeMap: make(map[int]Store),
	}
	strategy.Dcs = append(strategy.Dcs, mockData.Dcs...)

	// 初始化节点列表
	stores = make([]Store, 0)
	for i := range mockData.Dcs {
		dc := mockData.Dcs[i]
		for j := range dc.Rocks {
			rock := dc.Rocks[j]
			for k := range rock.Stores {
				store := rock.Stores[k]
				stores = append(stores, store)
				strategy.NodeMap[store.ID]=store
			}
		}
	}

}

// 测试是否能正确从json配置文件中读取节点分布
func TestMockData(t *testing.T) {
	data, err := MockData()
	if err != nil {
		t.Fatal(err)
		t.Fail()
	}
	PrintStores(data)
}

// 测试是否能正确地按照策略分配副本存储节点
func TestStrategy_TryAllocate(t *testing.T) {
	stores := make([]Store, 0)
	stgy := &Strategy{
		Dcs: make([]DC, 0),
	}
	data, err := MockData()
	if err != nil {
		t.Fatal(err)
		//	t.Fail()
	}
	stgy.Dcs = append(stgy.Dcs, data.Dcs...)

	// 初始化节点集合
	for i := range data.Dcs {
		dc := data.Dcs[i]
		for j := range dc.Rocks {
			rock := dc.Rocks[j]
			for k := range rock.Stores {
				store := rock.Stores[k]
				stores = append(stores, store)
			}
		}
	}

	region:= stgy.TryAllocate(stores)
	fmt.Println(region.Replicas)
}

func TestRemoveReptElem(t *testing.T) {
	var arr = []int{0, 0, 0}
	fmt.Println(RemoveReptElem(arr))
}

// 测试能否正确地对副本进行分配
func TestCheck(t *testing.T) {
	Init()
	//	fmt.Println(stores)

	region := Check(stores, Region{}, *strategy)
	fmt.Println(region)
}

// 测试含有重复节点Region (3节点相同 AAA式) 的3副本重新分配
func TestCheck2(t *testing.T) {
	Init()

	region := Check(stores, Region{Replicas:[]int{3,3,3}}, *strategy)
	fmt.Println(region)
}

// 测试含有重复节点Region (2同1异 ABB式)的3副本重新分配 去重后两副本在同一DC下
func TestCheck3(t *testing.T) {
	Init()
	region := Check(stores, Region{Replicas:[]int{2,3,3}}, *strategy)
	fmt.Println(region)
}

// 测试含有重复节点Region (ABB式) 的3副本重新分配  去重后两副本在不同DC下
func TestCheck4(t *testing.T) {
	Init()
	region := Check(stores, Region{Replicas:[]int{1,9,9}}, *strategy)
	fmt.Println(region)
}

// 测试含有相同Rack的3副本重新分配  3副本都在同一Rack下
func TestCheck5(t *testing.T) {
	Init()
	region := Check(stores, Region{Replicas:[]int{0,1,2}}, *strategy)
	fmt.Println(region)
}

// 测试含有相同Rack的3副本重新分配，去重后剩下两个节点在同一DC
func TestCheck6(t *testing.T) {
	Init()
	region := Check(stores, Region{Replicas:[]int{0,2,6}}, *strategy)
	fmt.Println(region)
}

// 测试含有相同Rack的3副本重新分配，去重后剩下两个节点在不同一DC
func TestCheck7(t *testing.T) {
	Init()
	region := Check(stores, Region{Replicas:[]int{0,2,15}}, *strategy)
	fmt.Println(region)
}

// 不含有相同Rack但是都在同一Dc 的3副本重新分配
func TestCheck8(t *testing.T) {
	Init()
	region := Check(stores, Region{Replicas:[]int{0,3,6}}, *strategy)
	fmt.Println(region)
}

// 测试合法副本分布
func TestCheck9(t *testing.T) {
	Init()
	region := Check(stores, Region{Replicas:[]int{0,6,15}}, *strategy)
	fmt.Println(region)
}

