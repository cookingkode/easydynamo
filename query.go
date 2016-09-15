package easydynamo

import (
	"github.com/AdRoll/goamz/dynamodb"
	"reflect"
)

type Query struct {
	t                 *Table
	q                 *dynamodb.UntypedQuery
	keyComparisons    []dynamodb.AttributeComparison
	attribComparisons []dynamodb.AttributeComparison
}

func (t *Table) NewQuery() *Query {
	return &Query{
		t: t,
		q: dynamodb.NewQuery(t.tb),
	}
}

const (
	COMPARISON_EQUAL                    = "EQ"
	COMPARISON_NOT_EQUAL                = "NE"
	COMPARISON_LESS_THAN_OR_EQUAL       = "LE"
	COMPARISON_LESS_THAN                = "LT"
	COMPARISON_GREATER_THAN_OR_EQUAL    = "GE"
	COMPARISON_GREATER_THAN             = "GT"
	COMPARISON_ATTRIBUTE_EXISTS         = "NOT_NULL"
	COMPARISON_ATTRIBUTE_DOES_NOT_EXIST = "NULL"
	COMPARISON_CONTAINS                 = "CONTAINS"
	COMPARISON_DOES_NOT_CONTAIN         = "NOT_CONTAINS"
	COMPARISON_BEGINS_WITH              = "BEGINS_WITH"
	COMPARISON_IN                       = "IN"
	COMPARISON_BETWEEN                  = "BETWEEN"
)

func (qry *Query) AddKeyCondition(keyName, condition string, val interface{}) {
	qry.keyComparisons = append(qry.keyComparisons, getComparison(keyName, condition, val))
}

func (qry *Query) AddAttributeFilterCondition(keyName, condition string, val interface{}) {
	qry.attribComparisons = append(qry.attribComparisons, getComparison(keyName, condition, val))
}

func (qry *Query) Fire(limit int64) ([]map[string]*dynamodb.Attribute, error) {
	if limit > 0 {
		qry.q.AddLimit(limit)
	}

	qry.q.AddKeyConditions(qry.keyComparisons)
	if len(qry.attribComparisons) > 0 {
		qry.q.AddQueryFilter(qry.attribComparisons)
	}

	return dynamodb.RunQuery(qry.q, qry.t.tb)
}

func getComparison(keyName, condition string, val interface{}) dynamodb.AttributeComparison {
	var comparison *dynamodb.AttributeComparison

	switch reflect.TypeOf(val).Kind() {
	case reflect.String:
		comparison = dynamodb.NewStringAttributeComparison(keyName, condition, val.(string))
	case reflect.Bool:
		comparison = dynamodb.NewBinaryAttributeComparison(keyName, condition, val.(bool))
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int8, reflect.Int64:
		comparison = dynamodb.NewNumericAttributeComparison(keyName, condition, int64(val.(int)))
	}

	return *comparison
}
