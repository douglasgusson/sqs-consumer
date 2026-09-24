package consumer

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"sqs-consumer/internal/storage"
)

func ToRecord(queueURL string, m types.Message, receivedAt time.Time) storage.Record {
	r := storage.Record{
		QueueURL:               queueURL,
		ReceivedAt:             receivedAt,
		MessageID:              aws.ToString(m.MessageId),
		ReceiptHandle:          aws.ToString(m.ReceiptHandle),
		MD5OfBody:              aws.ToString(m.MD5OfBody),
		MD5OfMessageAttributes: aws.ToString(m.MD5OfMessageAttributes),
		Body:                   aws.ToString(m.Body),
		Attributes:             m.Attributes,
	}
	if len(m.MessageAttributes) > 0 {
		r.MessageAttributes = make(map[string]storage.MessageAttribute, len(m.MessageAttributes))
		for k, v := range m.MessageAttributes {
			r.MessageAttributes[k] = storage.MessageAttribute{
				DataType:    aws.ToString(v.DataType),
				StringValue: aws.ToString(v.StringValue),
				BinaryValue: v.BinaryValue,
			}
		}
	}
	return r
}
