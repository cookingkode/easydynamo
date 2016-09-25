package easydynamo

import (
	"github.com/AdRoll/goamz/dynamodb"
	"reflect"
	"strconv"
)

type Query struct {
	t                 *Table
	q                 *dynamodb.UntypedQuery
	keyComparisons    []dynamodb.AttributeComparison
	attribComparisons []dynamodb.AttributeComparison
	attributes        []dynamodb.Attribute
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

func (qry *Query) AddUpdateAttribute(keyName string, val interface{}) {
	qry.attributes = append(qry.attributes, getAttribute(keyName, val))
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
	//return qry.BatchRead(qry.q)
}

func (qry *Query) FireUpdate(hashk string, rangek string, action string) (bool, error) {

	if len(qry.attributes) > 0 {
		qry.q.AddUpdates(qry.attributes, action)
	}

	return qry.t.tb.UpdateAttributes(&dynamodb.Key{HashKey: hashk, RangeKey: rangek}, qry.attributes)
}

func (qry *Query) BatchRead(query dynamodb.ScanQuery) ([]map[string]*dynamodb.Attribute, error) {

	finalResults := make([]map[string]*dynamodb.Attribute, 0, 100)

	for {
		results, lastEvaluatedKey, err := qry.t.tb.QueryTable(query)
		if err != nil {
			return nil, err
		}
		for _, item := range results {
			finalResults = append(finalResults, item)
		}

		if lastEvaluatedKey == nil {
			break
		}
		query.AddExclusiveStartKey(lastEvaluatedKey)
	}

	return finalResults, nil
}

func getComparison(keyName, condition string, val interface{}) dynamodb.AttributeComparison {
	var comparison *dynamodb.AttributeComparison

	switch reflect.TypeOf(val).Kind() {
	case reflect.String:
		comparison = dynamodb.NewStringAttributeComparison(keyName, condition, val.(string))
	case reflect.Bool:
		comparison = dynamodb.NewBoolAttributeComparison(keyName, condition, val.(bool))
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int8, reflect.Int64:
		comparison = dynamodb.NewNumericAttributeComparison(keyName, condition, int64(val.(int)))
	}

	return *comparison
}

func getAttribute(keyName string, val interface{}) dynamodb.Attribute {
	var attribute *dynamodb.Attribute

	switch reflect.TypeOf(val).Kind() {
	case reflect.String:
		attribute = dynamodb.NewStringAttribute(keyName, val.(string))
	case reflect.Bool:
		attribute = dynamodb.NewBoolAttribute(keyName, strconv.FormatBool(val.(bool)))
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int8, reflect.Int64:
		attribute = dynamodb.NewNumericAttribute(keyName, strconv.FormatInt(int64(val.(int)), 10))
	}

	return *attribute
}
