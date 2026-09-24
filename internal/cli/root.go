package cli

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
	"sqs-consumer/internal/consumer"
	"sqs-consumer/internal/storage"
)

type options struct {
	queueURL          string
	endpoint          string
	region            string
	output            string
	workers           int
	maxMessages       int
	waitTime          int
	visibilityTimeout int
	noDelete          bool
	exitOnEmpty       bool
}

func (o options) validate() error {
	switch {
	case o.workers < 1:
		return errors.New("--workers deve ser >= 1")
	case o.maxMessages < 1 || o.maxMessages > 10:
		return errors.New("--max-messages deve estar entre 1 e 10")
	case o.waitTime < 0 || o.waitTime > 20:
		return errors.New("--wait-time deve estar entre 0 e 20")
	case o.visibilityTimeout < 0:
		return errors.New("--visibility-timeout deve ser >= 0")
	}
	return nil
}

func NewRootCmd() *cobra.Command {
	var o options

	cmd := &cobra.Command{
		Use:   "sqs-consumer",
		Short: "Consome uma fila SQS (LocalStack) e grava as mensagens em JSONL",
		Long: `Consome uma fila SQS usando um pool de workers com long polling e grava
cada mensagem (body, atributos de sistema e message attributes) em um
arquivo JSONL, para que possam ser reenviadas à fila posteriormente.`,
		Example: `  # Consome e remove da fila
  sqs-consumer -q http://localhost:4566/000000000000/minha-fila

  # Apenas copia (não remove), 8 workers, saída customizada
  sqs-consumer -q <url> -w 8 -o dump.jsonl --no-delete

  # Esvazia a fila e encerra
  sqs-consumer -q <url> --exit-on-empty`,
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return o.validate()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

			client, err := consumer.NewSQSClient(ctx, o.endpoint, o.region)
			if err != nil {
				return fmt.Errorf("carregando config AWS: %w", err)
			}

			writer, err := storage.NewJSONLWriter(o.output)
			if err != nil {
				return fmt.Errorf("abrindo arquivo de saída: %w", err)
			}
			defer func() {
				if cerr := writer.Close(); cerr != nil {
					logger.Error("erro ao fechar writer", slog.Any("error", cerr))
				}
			}()

			cfg := consumer.Config{
				QueueURL:          o.queueURL,
				Workers:           o.workers,
				MaxMessages:       int32(o.maxMessages),
				WaitTimeSeconds:   int32(o.waitTime),
				VisibilityTimeout: int32(o.visibilityTimeout),
				DeleteAfterWrite:  !o.noDelete,
				ExitOnEmpty:       o.exitOnEmpty,
			}

			orch := consumer.NewOrchestrator(client, writer, cfg, logger)

			stats, err := orch.Run(ctx)
			if err != nil {
				return fmt.Errorf("erro durante execução do orquestrador: %w", err)
			}

			logger.Info("resumo final",
				slog.Int64("received", stats.Received),
				slog.Int64("written", stats.Written),
				slog.Int64("deleted", stats.Deleted),
				slog.Int64("write_errors", stats.WriteErrors),
				slog.Int64("delete_errors", stats.DeleteErrors),
				slog.Int64("receive_errors", stats.ReceiveErrors),
			)

			return nil
		},
	}

	f := cmd.Flags()
	f.StringVarP(&o.queueURL, "queue-url", "q", "", "URL da fila SQS (obrigatório)")
	f.StringVarP(&o.endpoint, "endpoint", "e", "http://localhost:4566", "endpoint do LocalStack")
	f.StringVarP(&o.region, "region", "r", "us-east-1", "região AWS")
	f.StringVarP(&o.output, "output", "o", "messages.jsonl", "arquivo JSONL de saída (modo append)")
	f.IntVarP(&o.workers, "workers", "w", 4, "número de workers")
	f.IntVar(&o.maxMessages, "max-messages", 10, "mensagens por receive (1-10)")
	f.IntVar(&o.waitTime, "wait-time", 5, "long polling em segundos (0-20)")
	f.IntVar(&o.visibilityTimeout, "visibility-timeout", 0, "visibility timeout em segundos (0 = padrão da fila)")
	f.BoolVar(&o.noDelete, "no-delete", false, "não remove a mensagem da fila após gravar")
	f.BoolVar(&o.exitOnEmpty, "exit-on-empty", false, "encerra quando a fila esvaziar")

	_ = cmd.MarkFlagRequired("queue-url")
	return cmd
}
