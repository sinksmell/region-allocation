package main

import (
	"testing"
	"fmt"
)

// 测试是否能正确从json配置文件中读取节点分布
func TestMockData(t *testing.T) {
	data,err:=MockData()
	if err!=nil{
		t.Fatal(err)
		t.Fail()
	}

	PrintStores(data)
	//for _,dc:=range data.Dcs {
	//	fmt.Printf("%02d %s RocksId: \n",dc.ID,dc.Name)
	//	for _,rock:=range dc.Rocks {
	//		fmt.Printf("\t %d %s   Stores: \n",rock.ID,rock.Name)
	//		for _,store:=range rock.Stores  {
	//			fmt.Printf("\t\t StoreId: %02d  belongto: RockID %02d\n",store.ID,store.Rock.ID)
	//		}
	//	}
	//	fmt.Println("--------")
	//}
}


// 测试是否能正确地按照策略分配副本存储节点
func TestStrategy_TryAllocate(t *testing.T) {
	stores:=make([]Store,0)
	stgy:=&Strategy{
		Dcs:make([]DC,0),
	}
	data,err:=MockData()
	if err!=nil{
		t.Fatal(err)
	//	t.Fail()
	}
	stgy.Dcs=append(stgy.Dcs,data.Dcs...)

	// 初始化节点集合
	for i:=range data.Dcs {
		dc:=data.Dcs[i]
		for j:=range dc.Rocks {
			rock:=dc.Rocks[j]
			for k:=range rock.Stores  {
				store:=rock.Stores[k]
				stores=append(stores,store)
			}
		}
	}

	region, err := stgy.TryAllocate(stores)
	if err!=nil{
		t.Fatal(err)
	}

	fmt.Println(region.Replicas)
}



