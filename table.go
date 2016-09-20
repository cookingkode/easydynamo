package easydynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"time"
)

// Reproduced here to avoid clients including aws
type ProvisionedThroughputDescription struct {

	// The date and time of the last provisioned throughput decrease for this table.
	LastDecreaseDateTime *time.Time `type:"timestamp" timestampFormat:"unix"`

	// The date and time of the last provisioned throughput increase for this table.
	LastIncreaseDateTime *time.Time `type:"timestamp" timestampFormat:"unix"`

	// The number of provisioned throughput decreases for this table during this
	// UTC calendar day. For current maximums on provisioned throughput decreases,
	// see Limits (http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
	// in the Amazon DynamoDB Developer Guide.
	NumberOfDecreasesToday *int64 `min:"1" type:"long"`

	// The maximum number of strongly consistent reads consumed per second before
	// DynamoDB returns a ThrottlingException. Eventually consistent reads require
	// less effort than strongly consistent reads, so a setting of 50 ReadCapacityUnits
	// per second provides 100 eventually consistent ReadCapacityUnits per second.
	ReadCapacityUnits *int64 `min:"1" type:"long"`

	// The maximum number of writes consumed per second before DynamoDB returns
	// a ThrottlingException.
	WriteCapacityUnits *int64 `min:"1" type:"long"`
}

func (t *Table) UpdateIOPS(ReadCapacityUnits, WriteCapacityUnits int64) (*ProvisionedThroughputDescription, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	svc := dynamodb.New(sess, aws.NewConfig().WithRegion(t.db.regionName))

	params := &dynamodb.UpdateTableInput{
		TableName: aws.String(t.tableName), // Required
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(ReadCapacityUnits),  // Required
			WriteCapacityUnits: aws.Int64(WriteCapacityUnits), // Required
		},
	}
	resp, err := svc.UpdateTable(params)

	if err != nil {
		return nil, err
	}

	var ret ProvisionedThroughputDescription
	ret.LastDecreaseDateTime = resp.TableDescription.ProvisionedThroughput.LastDecreaseDateTime
	ret.LastIncreaseDateTime = resp.TableDescription.ProvisionedThroughput.LastIncreaseDateTime
	ret.NumberOfDecreasesToday = resp.TableDescription.ProvisionedThroughput.NumberOfDecreasesToday
	ret.ReadCapacityUnits = resp.TableDescription.ProvisionedThroughput.ReadCapacityUnits
	ret.WriteCapacityUnits = resp.TableDescription.ProvisionedThroughput.WriteCapacityUnits

	return &ret, nil

}
