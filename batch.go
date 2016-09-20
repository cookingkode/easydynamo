package easydynamo

import (
	"github.com/AdRoll/goamz/dynamodb"
)

type PutBatch struct {
	t    *Table
	keys []*dynamodb.Key
	vals []map[string]interface{}
}

func (t *Table) NewPutBatch() *PutBatch {
	return &PutBatch{t: t}
}

// Add add a key (consisting of hash and range values) and attributes (map of attribute name
// to vaules ) to the batch
func (b *PutBatch) Add(hashkey, rangekey string, attribs map[string]interface{}) {

	b.keys = append(b.keys, &dynamodb.Key{
		HashKey:  hashkey,
		RangeKey: rangekey})

	b.vals = append(b.vals, attribs)

}

func (b *PutBatch) GetKV() ([]*dynamodb.Key, []map[string]interface{}) {
	return b.keys, b.vals
}

// Fire executes the query
func (b *PutBatch) Fire() ([]error, error) {
	return b.t.tb.BatchPutDocument(b.keys, b.vals)
}

// MultiFire executes the query taking into consideration the Max batch size for put (25)
// Here it launches multiple parallel Put requests
// Care needs to be taken to make sure IOPS requirements are not exceeded
func (b *PutBatch) MultiFire() ([]error, []error) {

	numKeys := len(b.keys)
	nBatches := numKeys / dynamodb.MaxPutBatchSize
	if (numKeys % dynamodb.MaxPutBatchSize) > 0 {
		nBatches++
	}

	// execute in batches of mat Put Size
	errsC := make(chan []error)
	errC := make(chan error)

	for i := 0; i < nBatches; i++ {
		start := i * dynamodb.MaxPutBatchSize
		end := (i + 1) * dynamodb.MaxPutBatchSize
		if end > numKeys {
			end = numKeys
		}

		go func(errsChan chan []error, errChan chan error, start, end int) {
			keys := b.keys[start:end]
			vals := b.vals[start:end]

			errs, err := b.t.tb.BatchPutDocument(keys, vals)
			errsChan <- errs
			errChan <- err
		}(errsC, errC, start, end)
	}

	var (
		perItemErrors  []error
		perBatchErrors []error
	)

	for i := 0; i < nBatches; i++ {
		errs, err := <-errsC, <-errC
		perItemErrors = append(perItemErrors, errs...)
		perBatchErrors = append(perBatchErrors, err)
	}

	return perItemErrors, perBatchErrors
}

type GetBatch struct {
	t    *Table
	keys []*dynamodb.Key
}

func (t *Table) NewGetBatch() *GetBatch {
	return &GetBatch{}
}

func (b *GetBatch) AddKeyToGet(hashkey, rangekey string) {
	b.keys = append(b.keys, &dynamodb.Key{
		HashKey:  hashkey,
		RangeKey: rangekey})
}

func (b *GetBatch) Fire(consistentRead bool, out []map[string]interface{}) ([]error, error) {
	return b.t.tb.BatchGetDocument(b.keys, consistentRead, out)
}
