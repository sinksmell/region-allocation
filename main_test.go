package main

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
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
		NodeMap: make(map[int]Store),
	}

	// 初始化节点列表
	stores = make([]Store, 0)
	for i := range mockData.Dcs {
		dc := mockData.Dcs[i]
		for j := range dc.Rocks {
			rock := dc.Rocks[j]
			for k := range rock.Stores {
				store := rock.Stores[k]
				stores = append(stores, store)
				strategy.NodeMap[store.ID] = store
			}
		}
	}

}

// 测试是否能正确从json配置文件中读取节点分布
func TestMockData(t *testing.T) {
	fmt.Println("测试节点分布json解析:")
	data, err := MockData()
	if err != nil {
		t.Fatal(err)
		t.Fail()
	}
	PrintStores(data)
	fmt.Println()
}

/*// 测试是否能正确地按照策略分配副本存储节点
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
	fmt.Println("测试解析数据 + 从0分配")
	region:= stgy.Allocate(stores)
	fmt.Println(region.Replicas)
	fmt.Println()
}*/

func TestRemoveReptElem(t *testing.T) {
	fmt.Println("测试节点去重函数: ")
	var arr = []int{0, 0, 0}
	fmt.Println("原始数据: ", arr)
	fmt.Println("节点去重后: ", strategy.RemoveReptElem(arr))
	fmt.Println()
}

// 测试能否正确地对副本进行分配
func TestCheck(t *testing.T) {
	var arr = []int{}
	Init()
	fmt.Println("测试能否正确地对3副本进行分配 从0开始分配")
	fmt.Println("原始数据: ", arr)
	region := Check(stores, Region{arr}, *strategy)
	fmt.Println("分配后: ", region.Replicas)
	strategy.PrintRegion(&region)
	fmt.Println()
}

// 测试含有重复节点Region (3节点相同 AAA式) 的3副本重新分配
func TestCheck2(t *testing.T) {
	var arr = []int{3, 3, 3}
	Init()
	region := Region{arr}
	fmt.Println("测试含有重复节点Region (3节点相同 AAA式) 的3副本重新分配")
	fmt.Println("原始数据: ", arr)
	strategy.PrintRegion(&region)
	region = Check(stores, region, *strategy)
	fmt.Println("重新分配后: ", region.Replicas)
	strategy.PrintRegion(&region)
	fmt.Println()
}

// 测试含有重复节点Region (2同1异 ABB式)的3副本重新分配 去重后两副本在同一DC下(Rack不同)
func TestCheck3(t *testing.T) {
	var arr = []int{2, 3, 3}
	Init()
	region := Region{arr}
	fmt.Println("测试含有重复节点Region (2同1异 ABB式)的3副本重新分配 去重后两副本在同一DC下(Rack不同)")
	fmt.Println("原始数据: ", arr)
	strategy.PrintRegion(&region)
	region = Check(stores, region, *strategy)
	fmt.Println("重新分配后: ", region.Replicas)
	strategy.PrintRegion(&region)
	fmt.Println()
}

// 测试含有重复节点Region (ABB式) 的3副本重新分配  去重后两副本在不同DC下 (Rack必定不同)
func TestCheck4(t *testing.T) {
	var arr = []int{1, 9, 9}
	region := Region{arr}
	Init()
	fmt.Println("测试含有重复节点Region (ABB式) 的3副本重新分配 去重后两副本在不同DC下 (Rack必定不同)")
	fmt.Println("原始数据: ", arr)
	strategy.PrintRegion(&region)
	region = Check(stores, region, *strategy)
	fmt.Println("重新分配后: ", region.Replicas)
	strategy.PrintRegion(&region)
	fmt.Println()
}

// 测试含有相同Rack的3副本重新分配  3副本都在同一Rack下
func TestCheck5(t *testing.T) {
	var arr = []int{0, 1, 2}
	region := Region{arr}
	Init()
	fmt.Println("测试含有相同Rack的3副本重新分配  3副本都在同一Rack下")
	fmt.Println("原始数据: ", arr)
	strategy.PrintRegion(&region)
	region = Check(stores, region, *strategy)
	fmt.Println("重新分配后: ", region.Replicas)
	strategy.PrintRegion(&region)
	fmt.Println()
}

// 测试含有相同Rack的3副本重新分配，去重后剩下两个节点在同一DC
func TestCheck6(t *testing.T) {
	var arr = []int{0, 2, 6}
	region := Region{arr}
	Init()
	fmt.Println("测试含有相同Rack的3副本重新分配，去重后剩下两个节点在同一DC")
	fmt.Println("原始数据: ", arr)
	strategy.PrintRegion(&region)
	region = Check(stores, region, *strategy)
	fmt.Println("重新分配后: ", region.Replicas)
	strategy.PrintRegion(&region)
	fmt.Println()
}

// 测试含有相同Rack的3副本重新分配，去重后剩下两个节点在不同一DC
func TestCheck7(t *testing.T) {
	var arr = []int{0, 2, 15}
	region := Region{arr}
	Init()
	fmt.Println("测试含有相同Rack的3副本重新分配，去重后剩下两个节点在不同一DC")
	fmt.Println("原始数据: ", arr)
	strategy.PrintRegion(&region)
	region = Check(stores, region, *strategy)
	fmt.Println("重新分配后: ", region.Replicas)
	strategy.PrintRegion(&region)
	fmt.Println()
}

// 不含有相同Rack但是都在同一Dc 的3副本重新分配
func TestCheck8(t *testing.T) {
	var arr = []int{0, 3, 6}
	region := Region{arr}
	Init()
	fmt.Println("不含有相同Rack但是都在同一Dc 的3副本重新分配")
	fmt.Println("原始数据: ", arr)
	strategy.PrintRegion(&region)
	region = Check(stores, region, *strategy)
	fmt.Println("重新分配后: ", region.Replicas)
	strategy.PrintRegion(&region)
	fmt.Println()
}

// 测试连续的3个节点id
func TestCheck9(t *testing.T) {
	var arr = []int{6, 8, 15}
	region := Region{arr}
	Init()
	fmt.Println("测试连续的节点id上副本分配")
	fmt.Println("原始数据: ", arr)
	strategy.PrintRegion(&region)
	region = Check(stores, region, *strategy)
	fmt.Println("重新分配后: ", region.Replicas)
	strategy.PrintRegion(&region)
	fmt.Println()
}

// 生成随机副本分配情况测试
func TestCheck10(t *testing.T) {
	rd := rand.New(rand.NewSource(time.Now().UnixNano()))
	var arr = make([]int, 3)
	Init()
	for i := 0; i < len(arr); i++ {
		arr[i] = rd.Intn(len(stores))
	}
	region := Region{arr}
	fmt.Println("随机生成的待检测副本分布: ", region)
	strategy.PrintRegion(&region)
	region = Check(stores, region, *strategy)
	fmt.Println("调整后的副本分布: ", region)
	strategy.PrintRegion(&region)
}

// 测试合法副本分布
func TestCheck11(t *testing.T) {
	var arr = []int{0, 6, 15}
	region := Region{arr}
	Init()
	fmt.Println("测试合法副本分布")
	fmt.Println("原始数据: ", arr)
	strategy.PrintRegion(&region)
	region = Check(stores, region, *strategy)
	fmt.Println("重新分配后: ", region.Replicas)
	strategy.PrintRegion(&region)
	fmt.Println()
}
