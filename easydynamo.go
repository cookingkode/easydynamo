package easydynamo

import (
	"errors"
	"fmt"
	aws "github.com/AdRoll/goamz/aws"
	dynamodb "github.com/AdRoll/goamz/dynamodb"
	"strings"
)

type DB struct {
	ddbs *dynamodb.Server
}

type Table struct {
	db *DB
	tb *dynamodb.Table
}

var (
	DBNotAvailableError = errors.New("DB not available, check auth")
)

/*
type Key struct {
	dynamodb.Key
}
*/

func GetDB(region string) (*DB, error) {

	var (
		auth aws.Auth
		err  error
		reg  aws.Region
	)

	if strings.HasPrefix(region, "http") {
		// local Dynamondb
		reg = aws.Region{DynamoDBEndpoint: "http://127.0.0.1:8000"}
		auth = aws.Auth{AccessKey: "DUMMY_KEY", SecretKey: "DUMMY_SECRET"}
	} else {
		reg = aws.GetRegion(region)
		if auth, err = aws.EnvAuth(); err != nil {
			fmt.Println("[easydynamo] GetDB", err)
			return nil, err
		}
	}

	var db DB

	db.ddbs = dynamodb.New(auth, reg)
	return &db, nil
}

func (db *DB) GetTable(name string) (*Table, error) {

	if db == nil {
		return nil, DBNotAvailableError
	}

	var err error
	tableDescriptor, err := db.ddbs.DescribeTable(name)
	if err != nil {
		return nil, err
	}

	pk, err := tableDescriptor.BuildPrimaryKey()
	if err != nil {
		return nil, err
	}

	var table Table
	table.tb = db.ddbs.NewTable(name, pk)

	return &table, nil

}

func (t *Table) BatchPutDocument(keys []*dynamodb.Key, v interface{}) ([]error, error) {
	return t.tb.BatchPutDocument(keys, v)
}

func (t *Table) BatchGetDocument(keys []*dynamodb.Key, consistentRead bool, v interface{}) ([]error, error) {
	return t.tb.BatchGetDocument(keys, consistentRead, v)
}
