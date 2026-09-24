# sqs-consumer — Especificação do Projeto

CLI em Go que consome mensagens de uma fila SQS (LocalStack) usando um **worker pool** com long polling e as grava em um arquivo **JSONL**, preservando todos os dados (body, atributos de sistema e message attributes) para permitir o **reenvio posterior** à fila.

---

## 1. Objetivos e escopo

### 1.1 Objetivos
- Consumir uma fila SQS informada via linha de comando.
- Processar em paralelo com N workers (worker pool).
- Persistir cada mensagem em JSONL, sem perda de informação relevante para reenvio.
- Encerrar de forma graciosa (SIGINT/SIGTERM) sem perder mensagens já recebidas.
- Ter uma arquitetura testável (dependências injetadas por interfaces).

### 1.2 Fora de escopo (nesta versão)
- Reenvio das mensagens (`replay`) — ver [seção 14](#14-evolução-futura).
- Autenticação AWS real (usa credenciais estáticas `test/test`, adequadas ao LocalStack).
- Filas FIFO com garantia de ordenação entre workers (com múltiplas workers a ordem de gravação **não** é garantida).
- Processamento de negócio da mensagem: o programa apenas registra.

---

## 2. Requisitos

| Item | Requisito |
|---|---|
| Go | 1.22+ |
| AWS SDK | `github.com/aws/aws-sdk-go-v2` (`config`, `credentials`, `service/sqs`) |
| CLI | `github.com/spf13/cobra` |
| Logging | `log/slog` (stdlib) |
| Testes | `testing` (stdlib); `testify` opcional |
| Infra local | LocalStack com o serviço SQS habilitado |

---

## 3. Estrutura do projeto

```
sqs-consumer/
├── cmd/
│   └── sqs-consumer/
│       └── main.go              # Ponto de entrada (Entrypoint)
├── internal/
│   ├── cli/
│   │   └── root.go              # Configuração do Cobra e injeção de dependências
│   ├── consumer/
│   │   ├── orchestrator.go      # Worker Pool e criação do client AWS SDK
│   │   ├── worker.go            # Lógica de polling (Receive) e processamento (Delete)
│   │   └── mapper.go            # Tradução types.Message (AWS) → storage.Record
│   └── storage/
│       ├── jsonl_writer.go      # I/O thread-safe (Mutex)
│       └── models.go            # Estruturas de dados (Record, MessageAttribute)
├── go.mod
└── go.sum
```

Arquivos de apoio recomendados (opcionais): `Makefile`, `docker-compose.yml` (LocalStack), `README.md`, arquivos `*_test.go` ao lado do código testado.

### 3.1 Regra de dependência entre pacotes

```
cmd/sqs-consumer ──► internal/cli ──► internal/consumer ──► internal/storage (models)
                          │
                          └────────────► internal/storage (jsonl_writer)
```

- `storage` **não** importa nenhum outro pacote interno.
- `consumer` depende de `storage` **apenas** pelo tipo `storage.Record`; a escrita é feita através da interface `RecordWriter` (definida em `consumer`), nunca pelo tipo concreto.
- `cli` é o único pacote que conhece as implementações concretas e as conecta (composition root).
- `main.go` não contém lógica de negócio.

---

## 4. Especificação por arquivo

### 4.1 `cmd/sqs-consumer/main.go`

**Responsabilidade:** entrypoint mínimo.

- Criar o contexto raiz com `signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)`.
- Chamar `cli.NewRootCmd().ExecuteContext(ctx)`.
- Em caso de erro: imprimir `erro: <mensagem>` em `stderr` e sair com código `1`.

```go
func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    if err := cli.NewRootCmd().ExecuteContext(ctx); err != nil {
        fmt.Fprintln(os.Stderr, "erro:", err)
        os.Exit(1)
    }
}
```

> `os.Exit` não executa `defer`; como `stop()` é apenas limpeza de sinal ao fim do processo, isso é aceitável aqui.

---

### 4.2 `internal/cli/root.go`

**Responsabilidade:** definição do comando Cobra, flags, validação e **injeção de dependências**.

**Comando raiz:** `sqs-consumer` (sem subcomandos nesta versão).

- `Args: cobra.NoArgs`
- `SilenceUsage: true` e `SilenceErrors: true` (o `main` imprime o erro; o usage só aparece em erros de flag).
- `MarkFlagRequired("queue-url")`.
- `PreRunE`: valida as opções (ver 4.2.2).
- `RunE`: monta as dependências e chama o orquestrador.

#### 4.2.1 Flags

| Flag | Short | Tipo | Default | Descrição |
|---|---|---|---|---|
| `--queue-url` | `-q` | string | — (**obrigatória**) | URL da fila SQS |
| `--endpoint` | `-e` | string | `http://localhost:4566` | Endpoint do LocalStack |
| `--region` | `-r` | string | `us-east-1` | Região AWS |
| `--output` | `-o` | string | `messages.jsonl` | Arquivo JSONL de saída (modo append) |
| `--workers` | `-w` | int | `4` | Número de workers |
| `--max-messages` | — | int | `10` | Mensagens por `ReceiveMessage` (1–10) |
| `--wait-time` | — | int | `5` | Long polling em segundos (0–20) |
| `--visibility-timeout` | — | int | `0` | Visibility timeout em segundos (`0` = padrão da fila) |
| `--no-delete` | — | bool | `false` | Não remove a mensagem da fila após gravar (modo *peek*) |
| `--exit-on-empty` | — | bool | `false` | Encerra quando a fila esvaziar |

#### 4.2.2 Validação (`PreRunE`)

| Condição | Erro |
|---|---|
| `workers < 1` | `--workers deve ser >= 1` |
| `max-messages` fora de 1–10 | `--max-messages deve estar entre 1 e 10` |
| `wait-time` fora de 0–20 | `--wait-time deve estar entre 0 e 20` |
| `visibility-timeout < 0` | `--visibility-timeout deve ser >= 0` |
| `queue-url` vazio | tratado pelo Cobra (flag obrigatória) |

#### 4.2.3 Composição (`RunE`)

Ordem de montagem:

1. `client, err := consumer.NewSQSClient(ctx, endpoint, region)`
2. `writer, err := storage.NewJSONLWriter(output)`
3. `defer writer.Close()` — o erro do `Close` deve ser propagado (ver 4.6).
4. `orch := consumer.NewOrchestrator(client, writer, cfg, logger)`
5. `stats, err := orch.Run(ctx)`
6. Logar o resumo final (`stats`).

O `logger` é um `*slog.Logger` com `TextHandler` em `stderr`.

---

### 4.3 `internal/consumer/orchestrator.go`

**Responsabilidade:** criar o client AWS e gerenciar o ciclo de vida do worker pool.

#### 4.3.1 Tipos e interfaces

```go
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
    Received     int64
    Written      int64
    Deleted      int64
    WriteErrors  int64
    DeleteErrors int64
    ReceiveErrors int64
}
```

Os contadores de `Stats` são mantidos internamente com `atomic.Int64` e convertidos para `Stats` (valores simples) ao final de `Run`.

#### 4.3.2 Funções

```go
// NewSQSClient cria o client apontando para o endpoint do LocalStack.
func NewSQSClient(ctx context.Context, endpoint, region string) (*sqs.Client, error)

func NewOrchestrator(client SQSAPI, w RecordWriter, cfg Config, log *slog.Logger) *Orchestrator

// Run inicia cfg.Workers workers e bloqueia até todas terminarem.
func (o *Orchestrator) Run(ctx context.Context) (Stats, error)
```

**`NewSQSClient`**
- `config.LoadDefaultConfig` com `WithRegion(region)` e `WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", ""))`.
- `sqs.NewFromConfig(cfg, func(o *sqs.Options){ o.BaseEndpoint = aws.String(endpoint) })`.

**`Run`**
1. Loga o início (fila, workers, delete on/off).
2. Dispara `cfg.Workers` goroutines com `sync.WaitGroup`, cada uma executando `worker.run(ctx)`.
3. Aguarda `wg.Wait()`.
4. Retorna `Stats` e `nil`. Cancelamento por sinal **não** é erro.

---

### 4.4 `internal/consumer/worker.go`

**Responsabilidade:** loop de polling e processamento de cada mensagem.

```go
type worker struct {
    id     int
    client SQSAPI
    writer RecordWriter
    cfg    Config
    stats  *counters
    log    *slog.Logger
}

func (w *worker) run(ctx context.Context)
```

#### 4.4.1 Requisição de recebimento

`ReceiveMessageInput` montado **uma vez** por worker:

| Campo | Valor |
|---|---|
| `QueueUrl` | `cfg.QueueURL` |
| `MaxNumberOfMessages` | `cfg.MaxMessages` |
| `WaitTimeSeconds` | `cfg.WaitTimeSeconds` |
| `MessageSystemAttributeNames` | `[]types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll}` |
| `MessageAttributeNames` | `[]string{"All"}` |
| `VisibilityTimeout` | `cfg.VisibilityTimeout` (somente se `> 0`) |

> Pedir `All` nos dois campos é o que garante que `attributes` e `messageAttributes` venham completos.

#### 4.4.2 Fluxo do loop

```
loop:
  se ctx cancelado → return
  out, err := ReceiveMessage(ctx, input)
  se err:
      se ctx cancelado → return
      loga erro; ReceiveErrors++
      aguarda 2s (ou ctx.Done()) e continua
  se len(out.Messages) == 0:
      se ExitOnEmpty → return
      continua
  para cada mensagem m:
      Received++
      rec := ToRecord(cfg.QueueURL, m, time.Now().UTC())
      se writer.Write(rec) falhar:
          loga erro; WriteErrors++
          NÃO deleta; continua   // reaparece após visibility timeout
      Written++
      se DeleteAfterWrite:
          dctx, cancel := context.WithTimeout(context.Background(), 5s)
          DeleteMessage(dctx, {QueueUrl, ReceiptHandle})
          cancel()
          se falhar: loga; DeleteErrors++  senão: Deleted++
```

#### 4.4.3 Regras de comportamento

1. **Write-then-delete:** a mensagem só é removida da fila após gravação bem-sucedida.
2. **Delete com contexto independente:** o `DeleteMessage` usa `context.Background()` com timeout de 5s, para que uma mensagem **já gravada** seja removida mesmo durante o shutdown (evita duplicar no próximo run).
3. **Shutdown:** ao cancelar o `ctx`, o `ReceiveMessage` em andamento é abortado; mensagens de um lote já recebido continuam sendo processadas até o fim antes de a worker sair.
4. **Backoff de erro de receive:** espera fixa de 2s (`select` com `ctx.Done()`).
5. **`--exit-on-empty`:** cada worker encerra na **primeira** resposta vazia; o programa termina quando todas encerrarem.
6. Falha de delete **não** interrompe a worker (a mensagem será reentregue e ficará duplicada no JSONL — comportamento *at-least-once*).

---

### 4.5 `internal/consumer/mapper.go`

**Responsabilidade:** traduzir tipos da AWS para o domínio de `storage`.

```go
func ToRecord(queueURL string, m types.Message, receivedAt time.Time) storage.Record
```

Mapeamento:

| `types.Message` | `storage.Record` |
|---|---|
| — (parâmetro) | `QueueURL` |
| — (parâmetro) | `ReceivedAt` |
| `MessageId` | `MessageID` |
| `ReceiptHandle` | `ReceiptHandle` |
| `MD5OfBody` | `MD5OfBody` |
| `MD5OfMessageAttributes` | `MD5OfMessageAttributes` |
| `Body` | `Body` |
| `Attributes` (`map[string]string`) | `Attributes` |
| `MessageAttributes` (`map[string]types.MessageAttributeValue`) | `MessageAttributes` (`map[string]storage.MessageAttribute`) |

Regras:
- Ponteiros são convertidos com `aws.ToString` (nil → `""`).
- `MessageAttributes` é `nil` quando a mensagem não tem atributos (o campo é omitido no JSON).
- `BinaryValue` é copiado como `[]byte` (serializado em base64 pelo `encoding/json`).
- `receivedAt` é recebido por parâmetro para tornar a função **determinística e testável**.
- A função é pura: sem I/O, sem relógio, sem logging.

---

### 4.6 `internal/storage/models.go`

**Responsabilidade:** estruturas de dados persistidas.

```go
// MessageAttribute espelha o formato aceito por SendMessage.
type MessageAttribute struct {
    DataType    string `json:"dataType"`
    StringValue string `json:"stringValue,omitempty"`
    BinaryValue []byte `json:"binaryValue,omitempty"` // base64 no JSON
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
```

Este pacote **não** importa o AWS SDK.

---

### 4.7 `internal/storage/jsonl_writer.go`

**Responsabilidade:** escrita thread-safe de registros em JSONL.

```go
type JSONLWriter struct {
    mu     sync.Mutex
    f      *os.File
    closed bool
}

func NewJSONLWriter(path string) (*JSONLWriter, error)
func (w *JSONLWriter) Write(r Record) error
func (w *JSONLWriter) Close() error
```

**`NewJSONLWriter`**
- Abre com `os.O_CREATE|os.O_APPEND|os.O_WRONLY` e permissão `0o644`.
- O modo *append* permite execuções sucessivas no mesmo arquivo.

**`Write`**
1. `json.Marshal(r)` **fora** do lock (não bloqueia as outras workers).
2. Acrescenta `'\n'`.
3. Adquire o mutex e faz **uma única** chamada `f.Write(b)`, garantindo que cada linha seja escrita inteira e sem intercalação.
4. Retorna erro se o writer já estiver fechado.

**`Close`**
- Adquire o mutex, executa `f.Sync()` e `f.Close()`.
- **Idempotente:** chamadas subsequentes retornam `nil`.
- Se `Sync` falhar, ainda tenta fechar o arquivo e retorna o erro.

> `Sync` é feito apenas no `Close` (e não a cada escrita) por desempenho. Em queda abrupta do processo, as últimas linhas ainda no page cache do SO podem ser perdidas — nesse caso as mensagens já teriam sido deletadas da fila. Se essa garantia for crítica, ver [seção 14](#14-evolução-futura) (`--fsync`).

---

## 5. Formato do arquivo JSONL

Uma mensagem por linha, UTF-8, separador `\n`. Exemplo (formatado em várias linhas apenas para leitura):

```json
{
  "queueUrl": "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/minha-fila",
  "receivedAt": "2026-09-24T12:00:00.123456789Z",
  "messageId": "b2f1c6a0-...",
  "receiptHandle": "MWQ2...",
  "md5OfBody": "9e107d9d372bb6826bd81d3542a419d6",
  "md5OfMessageAttributes": "3b1c...",
  "body": "{\"pedidoId\":123}",
  "attributes": {
    "SentTimestamp": "1758715200000",
    "ApproximateReceiveCount": "1",
    "ApproximateFirstReceiveTimestamp": "1758715200500",
    "SenderId": "000000000000"
  },
  "messageAttributes": {
    "tipo": { "dataType": "String", "stringValue": "pedido" },
    "versao": { "dataType": "Number", "stringValue": "2" },
    "blob": { "dataType": "Binary", "binaryValue": "aGVsbG8=" }
  }
}
```

### 5.1 Compatibilidade com reenvio (`SendMessage`)

| Campo no JSONL | Uso no reenvio |
|---|---|
| `body` | `MessageBody` |
| `messageAttributes.*` | `MessageAttributes` (`DataType`, `StringValue`, `BinaryValue`) — mapeamento 1:1 |
| `attributes.MessageGroupId` | `MessageGroupId` (filas FIFO) |
| `attributes.MessageDeduplicationId` | `MessageDeduplicationId` (filas FIFO) |
| `attributes.AWSTraceHeader` | `MessageSystemAttributes["AWSTraceHeader"]` (opcional) |
| `queueUrl` | Fila de origem (informativo) |
| `messageId`, `receiptHandle`, `md5*`, `receivedAt` | Apenas registro/auditoria — **não** reenviáveis |
| `attributes.SentTimestamp`, `ApproximateReceiveCount`, `ApproximateFirstReceiveTimestamp`, `SenderId` | Apenas registro — atributos de sistema não são reenviáveis |

---

## 6. Concorrência

- N goroutines de worker, cada uma com seu próprio loop de long polling.
- Um único `JSONLWriter` compartilhado; a serialização das escritas é feita por `sync.Mutex`.
- Contadores de estatística via `sync/atomic`.
- O client SQS do SDK v2 é seguro para uso concorrente.
- Sem canais entre workers: a simplicidade é preferida enquanto a escrita não for o gargalo.

---

## 7. Ciclo de vida e shutdown

1. `SIGINT`/`SIGTERM` cancela o contexto raiz.
2. Workers param de fazer novos `ReceiveMessage`.
3. Lotes já recebidos são gravados e deletados (delete com contexto independente, timeout de 5s).
4. `Orchestrator.Run` retorna ao fim do `wg.Wait()`.
5. O `RunE` fecha o writer (`Sync` + `Close`) e loga o resumo.
6. Processo sai com código `0`.

---

## 8. Tratamento de erros

| Situação | Comportamento |
|---|---|
| Flag inválida / faltando | Cobra imprime erro + usage; exit `1` |
| Falha ao criar client AWS | Erro retornado por `RunE`; exit `1` |
| Falha ao abrir o arquivo de saída | Erro retornado por `RunE`; exit `1` |
| Erro em `ReceiveMessage` | Log + backoff de 2s; worker continua |
| Erro ao gravar no JSONL | Log; mensagem **não** é deletada; worker continua |
| Erro em `DeleteMessage` | Log; worker continua (possível duplicata futura) |
| Erro em `writer.Close()` | Propagado como erro de `RunE`; exit `1` |
| Cancelamento por sinal | Não é erro; exit `0` |

Erros devem ser encapsulados com `fmt.Errorf("contexto: %w", err)`.

---

## 9. Logging

- `log/slog` com `TextHandler` em `stderr`; `stdout` fica livre.
- Atributos estruturados fixos: `worker` (id), `message_id`, `queue_url` quando aplicável.
- Níveis: `INFO` (início, fim, resumo), `ERROR` (falhas de receive/write/delete). Não logar o `body` da mensagem.
- Resumo final: `received`, `written`, `deleted`, `write_errors`, `delete_errors`, `receive_errors`.

---

## 10. Build e execução

### 10.1 Setup do módulo

```bash
go mod init sqs-consumer
go get github.com/aws/aws-sdk-go-v2/aws \
       github.com/aws/aws-sdk-go-v2/config \
       github.com/aws/aws-sdk-go-v2/credentials \
       github.com/aws/aws-sdk-go-v2/service/sqs \
       github.com/spf13/cobra
go mod tidy
```

Imports internos usam o prefixo do módulo, ex.: `sqs-consumer/internal/storage`.

### 10.2 Build

```bash
go build -o bin/sqs-consumer ./cmd/sqs-consumer
```

### 10.3 Exemplos de uso

```bash
# Consome e remove da fila
./bin/sqs-consumer -q http://localhost:4566/000000000000/minha-fila

# Apenas copia (não remove), 8 workers, saída customizada
./bin/sqs-consumer -q <url> -w 8 -o dump.jsonl --no-delete

# Esvazia a fila e encerra
./bin/sqs-consumer -q <url> --exit-on-empty
```

### 10.4 Ambiente local (LocalStack)

`docker-compose.yml`:

```yaml
services:
  localstack:
    image: localstack/localstack
    ports:
      - "4566:4566"
    environment:
      - SERVICES=sqs
```

Criar fila e enviar mensagem com atributos:

```bash
awslocal sqs create-queue --queue-name minha-fila

awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/minha-fila \
  --message-body '{"pedidoId":123}' \
  --message-attributes '{"tipo":{"DataType":"String","StringValue":"pedido"}}'
```

### 10.5 Makefile sugerido

| Target | Ação |
|---|---|
| `build` | `go build -o bin/sqs-consumer ./cmd/sqs-consumer` |
| `test` | `go test -race ./...` |
| `vet` | `go vet ./...` |
| `lint` | `golangci-lint run` (opcional) |
| `up` / `down` | `docker compose up -d` / `down` |

---

## 11. Estratégia de testes

### 11.1 Unitários

| Pacote | Casos |
|---|---|
| `consumer/mapper` | Mensagem completa; sem `MessageAttributes` (campo `nil`); atributo `Binary`; ponteiros `nil`; `receivedAt` propagado |
| `storage/jsonl_writer` | Uma linha por `Write`; cada linha é JSON válido; escrita concorrente (ex.: 50 goroutines × 100 registros) sem linhas corrompidas (`go test -race`); modo append preserva conteúdo anterior; `Close` idempotente; `Write` após `Close` retorna erro |
| `consumer/worker` | Com `SQSAPI` fake: grava e deleta em caso de sucesso; **não** deleta se o write falhar; `--no-delete` não chama `DeleteMessage`; `ExitOnEmpty` encerra na resposta vazia; erro de receive → backoff e continua; cancelamento do `ctx` encerra o loop; delete ainda ocorre após cancelamento |
| `consumer/orchestrator` | Sobe exatamente N workers; agrega `Stats` corretamente |
| `cli` | Validação de flags (valores limites e inválidos); flag obrigatória ausente |

### 11.2 Integração (opcional)

Com LocalStack (docker-compose ou testcontainers), sob build tag `integration`:
1. Criar fila e enviar mensagens com `MessageAttributes` (String, Number, Binary).
2. Executar o consumer com `--exit-on-empty`.
3. Verificar que o JSONL contém todas as mensagens e atributos, e que a fila ficou vazia.
4. Repetir com `--no-delete` e verificar que a fila mantém as mensagens (após o visibility timeout).

---

## 12. Critérios de aceite

- [ ] `sqs-consumer` sem `--queue-url` falha com mensagem clara.
- [ ] Todas as mensagens enviadas aparecem no JSONL, uma por linha, com `body`, `attributes` e `messageAttributes`.
- [ ] `messageAttributes` do JSONL pode ser passado diretamente ao `SendMessage` sem transformação além da troca de nomes de campo.
- [ ] Nenhuma linha do arquivo fica corrompida com `--workers 16` (validado com `-race`).
- [ ] Nenhuma mensagem é deletada da fila sem ter sido gravada.
- [ ] `Ctrl+C` encerra em poucos segundos, fecha o arquivo e imprime o resumo.
- [ ] `--no-delete` não remove nenhuma mensagem.
- [ ] `--exit-on-empty` encerra após esvaziar a fila.
- [ ] `go vet ./...` e `go test -race ./...` passam.

---

## 13. Decisões de projeto e limitações

| Decisão | Justificativa |
|---|---|
| Layout `cmd/` + `internal/` | Separa entrypoint da lógica e impede uso externo dos pacotes |
| Interfaces `SQSAPI` e `RecordWriter` definidas em `consumer` | Consumidor define o contrato; facilita mocks |
| Write-then-delete | Prioriza não perder dados; custo: possíveis duplicatas (*at-least-once*) |
| Um `Write` por linha sob mutex | Garante integridade de linha sem overhead de canais |
| `Sync` só no `Close` | Desempenho; risco documentado na seção 4.7 |
| Credenciais estáticas `test/test` | Alvo é LocalStack |
| `receivedAt` injetado no mapper | Determinismo em testes |

**Limitações conhecidas**
- Ordem das mensagens no arquivo não é garantida com mais de uma worker.
- Duplicatas podem ocorrer (falha de delete, visibility timeout curto, filas standard).
- `receiptHandle` no arquivo é apenas informativo: expira e não serve para reenvio.
- O tamanho máximo de mensagem segue o limite do SQS; não há tratamento especial para mensagens grandes (sem uso do Extended Client / S3).

---

## 14. Evolução futura

1. **Subcomando `replay`:** ler o JSONL e reenviar com `SendMessageBatch` (lotes de 10), com `--queue-url` de destino, `--rate`, `--dry-run` e suporte a FIFO (`MessageGroupId`/`MessageDeduplicationId`). O layout atual já comporta um novo pacote `internal/replayer` e um `cli/replay.go`.
2. **`--fsync`:** `Sync` periódico ou a cada N registros.
3. **Rotação de arquivo** por tamanho ou tempo.
4. **Suporte a AWS real:** flag `--use-default-credentials` para usar a cadeia padrão de credenciais.
5. **Backoff exponencial** com jitter no erro de receive.
6. **Métricas** (contadores expostos em endpoint HTTP ou log periódico).
