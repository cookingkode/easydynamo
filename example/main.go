package main

import (
	"flag"
	"fmt"
	"github.com/AdRoll/goamz/dynamodb"
	"github.com/cookingkode/easydynamo"
	"math/rand"
	"strconv"
	_ "sync"
	"time"
)

const (
	MaxPutBatchSize = 25
)

var (
	numKeys    = flag.Int("nk", 5, "number of keys")
	doWrite    = flag.Bool("w", false, "do write")
	doRead     = flag.Bool("r", false, "do read")
	doQuery    = flag.Bool("q", false, " do query")
	userid     = flag.String("u", "UserX", "userid ")
	updateIops = flag.Bool("i", false, " update iops")
	readIops   = flag.Int64("ri", 5, "read capacity units")
	writeIops  = flag.Int64("wi", 5, "write capacity units")
)

func main() {
	flag.Parse()

	db, err := easydynamo.GetDB("ap-south-1")
	if err != nil {
		panic(err)
	}

	tbl, err := db.GetTable("Activities")
	if err != nil {
		panic(err)
	}

	var (
		keys []*dynamodb.Key
		ins  []map[string]interface{}
		outs []map[string]interface{}
	)

	if *updateIops {
		fmt.Println(tbl.UpdateIOPS(*readIops, *writeIops))
		return
	}

	if *doWrite {
		populate(tbl, *userid, *numKeys)

	}

	if *doRead {
		outs = read(tbl, keys, true)

		for i := 0; i < *numKeys; i++ {
			fmt.Println("[check] in ", ins[i], " out ", outs[i])
		}
	}

	if *doQuery {
		query(tbl, *userid)
	}

}

func populate(tbl *easydynamo.Table, userid string, numKeys int) ([]*dynamodb.Key, []map[string]interface{}) {

	batch := tbl.NewPutBatch()

	for i := 0; i < numKeys; i++ {
		is := strconv.Itoa(i)

		rangeKy := fmt.Sprintf(
			"%v#%s#%s#%s", time.Now().Unix()+int64(i), randSeq(8), "t_x", randSeq(15))

		attrib := map[string]interface{}{
			"Detail":    "Detail" + is,
			"IntDetail": i,
		}

		batch.Add(userid, rangeKy, attrib)

	}

	start := time.Now()
	errs, _ := batch.Fire()

	errs, batchErrs := batch.MultiFire()

	for _, batchErr := range batchErrs {
		if batchErr != nil {
			panic(batchErr)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("populate took %v\n", elapsed)

	for i := 0; i < numKeys; i++ {
		if errs[i] != nil {
			panic(errs[i])
		}

	}

	return batch.GetKV()
}

func read(tbl *easydynamo.Table, keys []*dynamodb.Key, isConsistent bool) []map[string]interface{} {
	outs := make([]map[string]interface{}, *numKeys)

	errs, err := tbl.BatchGetDocument(keys, true, outs)
	if err != nil {
		panic(err)
	}

	for i := 0; i < *numKeys; i++ {
		if errs[i] != nil {
			panic(errs[i])
		}
	}

	return outs
}

func query(tbl *easydynamo.Table, userid string) {
	start := time.Now()

	q := tbl.NewQuery()
	q.AddKeyCondition("userid", easydynamo.COMPARISON_EQUAL, userid)
	x, _ := q.Fire(0)
	elapsed := time.Since(start)
	fmt.Printf("query took %v\n", elapsed)
	fmt.Printf("query returned %v items\n", len(x))

	for _, v := range x {
		fmt.Printf("%v %v\n", v["gai"].Value, v["userid"].Value)
	}

}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
