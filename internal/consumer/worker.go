package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type worker struct {
	id     int
	client SQSAPI
	writer RecordWriter
	cfg    Config
	stats  *counters
	log    *slog.Logger
}

func (w *worker) run(ctx context.Context) {
	w.log.Info("worker iniciado", slog.Int("worker", w.id))
	defer w.log.Info("worker finalizado", slog.Int("worker", w.id))

	input := &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(w.cfg.QueueURL),
		MaxNumberOfMessages:         w.cfg.MaxMessages,
		WaitTimeSeconds:             w.cfg.WaitTimeSeconds,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		MessageAttributeNames:       []string{"All"},
	}
	if w.cfg.VisibilityTimeout > 0 {
		input.VisibilityTimeout = w.cfg.VisibilityTimeout
	}

	for {
		if ctx.Err() != nil {
			return
		}

		out, err := w.client.ReceiveMessage(ctx, input)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.log.Error("erro no receive (retry em 2s)", slog.Int("worker", w.id), slog.Any("error", err))
			w.stats.receiveErrors.Add(1)

			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}

		if len(out.Messages) == 0 {
			if w.cfg.ExitOnEmpty {
				return
			}
			continue
		}

		for _, m := range out.Messages {
			w.stats.received.Add(1)

			rec := ToRecord(w.cfg.QueueURL, m, time.Now().UTC())
			if err := w.writer.Write(rec); err != nil {
				w.log.Error("erro gravando (mensagem NÃO removida)", slog.Int("worker", w.id), slog.String("message_id", aws.ToString(m.MessageId)), slog.Any("error", err))
				w.stats.writeErrors.Add(1)
				continue
			}
			w.stats.written.Add(1)

			if !w.cfg.DeleteAfterWrite {
				continue
			}

			dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := w.client.DeleteMessage(dctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(w.cfg.QueueURL),
				ReceiptHandle: m.ReceiptHandle,
			})
			cancel()

			if err != nil {
				w.log.Error("erro deletando", slog.Int("worker", w.id), slog.String("message_id", aws.ToString(m.MessageId)), slog.Any("error", err))
				w.stats.deleteErrors.Add(1)
			} else {
				w.stats.deleted.Add(1)
			}
		}
	}
}
