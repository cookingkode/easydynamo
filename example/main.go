package main

import (
	"fmt"
	"github.com/AdRoll/goamz/dynamodb"
	"github.com/cookingkode/easydynamo"
	"strconv"
)

func main() {
	fmt.Println("hello world")

	db, err := easydynamo.GetDB("ap-south-1")

	if err != nil {
		panic(err)
	}

	//tbl := easydynamo.GetDB("ap-south-1").GetTable("Activities")
	tbl, err := db.GetTable("Activities")

	if err != nil {
		panic(err)
	}

	/*
		Sample Table

		userid -> hash
		type -> sort
		id -> sort
		detail -> attrib
		intdetail -> attrib


	*/
	numKeys := 3
	keys := make([]*dynamodb.Key, 0, numKeys)
	ins := make([]map[string]interface{}, 0, numKeys)
	outs := make([]map[string]interface{}, numKeys)

	for i := 0; i < numKeys; i++ {
		is := strconv.Itoa(i)
		k := &dynamodb.Key{HashKey: "userid" + is}
		k.RangeKey = "randActType" + is + "_" + "randActId" + is
		in := map[string]interface{}{
			"Detail":    "Detail" + is,
			"IntDetail": i,
		}

		keys = append(keys, k)
		ins = append(ins, in)

	}

	errs, err := tbl.BatchPutDocument(keys, ins)
	if err != nil {
		panic(err)
	}
	for i := 0; i < numKeys; i++ {
		if errs[i] != nil {
			panic(errs[i])
		}

	}

	errs, err = tbl.BatchGetDocument(keys, true, outs)
	if err != nil {
		panic(err)
	}

	for i := 0; i < numKeys; i++ {
		if errs[i] != nil {
			panic(errs[i])
		}

		fmt.Println("[check] in ", ins[i], " out ", outs[i])
	}

}
