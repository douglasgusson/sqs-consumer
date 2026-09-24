package storage

import "time"

// MessageAttribute espelha o formato aceito por SendMessage.
// BinaryValue é serializado em base64 pelo encoding/json.
type MessageAttribute struct {
	DataType    string `json:"dataType"`
	StringValue string `json:"stringValue,omitempty"`
	BinaryValue []byte `json:"binaryValue,omitempty"`
}

// Record é uma linha do arquivo JSONL.
type Record struct {
	QueueURL               string                      `json:"queueUrl"`
	ReceivedAt             time.Time                   `json:"receivedAt"`
	MessageID              string                      `json:"messageId"`
	ReceiptHandle          string                      `json:"receiptHandle"`
	MD5OfBody              string                      `json:"md5OfBody"`
	MD5OfMessageAttributes string                      `json:"md5OfMessageAttributes,omitempty"`
	Body                   string                      `json:"body"`
	Attributes             map[string]string           `json:"attributes,omitempty"`
	MessageAttributes      map[string]MessageAttribute `json:"messageAttributes,omitempty"`
}
