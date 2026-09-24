package consumer

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"sqs-consumer/internal/storage"
)

// SQSAPI é o subconjunto do client SQS usado pelo consumer (facilita mocks).
type SQSAPI interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// RecordWriter é implementado por storage.JSONLWriter.
type RecordWriter interface {
	Write(r storage.Record) error
}

type Config struct {
	QueueURL          string
	Workers           int
	MaxMessages       int32
	WaitTimeSeconds   int32
	VisibilityTimeout int32 // 0 = padrão da fila
	DeleteAfterWrite  bool  // true por padrão (inverso de --no-delete)
	ExitOnEmpty       bool
}

type Stats struct {
	Received      int64
	Written       int64
	Deleted       int64
	WriteErrors   int64
	DeleteErrors  int64
	ReceiveErrors int64
}

type counters struct {
	received      atomic.Int64
	written       atomic.Int64
	deleted       atomic.Int64
	writeErrors   atomic.Int64
	deleteErrors  atomic.Int64
	receiveErrors atomic.Int64
}

type Orchestrator struct {
	client SQSAPI
	writer RecordWriter
	cfg    Config
	log    *slog.Logger
}

// NewSQSClient cria o client apontando para o endpoint do LocalStack.
func NewSQSClient(ctx context.Context, endpoint, region string) (*sqs.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		return nil, err
	}
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	}), nil
}

func NewOrchestrator(client SQSAPI, w RecordWriter, cfg Config, log *slog.Logger) *Orchestrator {
	return &Orchestrator{
		client: client,
		writer: w,
		cfg:    cfg,
		log:    log,
	}
}

// Run inicia cfg.Workers workers e bloqueia até todas terminarem.
func (o *Orchestrator) Run(ctx context.Context) (Stats, error) {
	o.log.Info("iniciando orchestrator",
		slog.String("queue_url", o.cfg.QueueURL),
		slog.Int("workers", o.cfg.Workers),
		slog.Bool("delete", o.cfg.DeleteAfterWrite),
	)

	var (
		wg sync.WaitGroup
		c  counters
	)

	for i := 1; i <= o.cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w := worker{
				id:     id,
				client: o.client,
				writer: o.writer,
				cfg:    o.cfg,
				stats:  &c,
				log:    o.log,
			}
			w.run(ctx)
		}(i)
	}

	wg.Wait()

	return Stats{
		Received:      c.received.Load(),
		Written:       c.written.Load(),
		Deleted:       c.deleted.Load(),
		WriteErrors:   c.writeErrors.Load(),
		DeleteErrors:  c.deleteErrors.Load(),
		ReceiveErrors: c.receiveErrors.Load(),
	}, nil
}
