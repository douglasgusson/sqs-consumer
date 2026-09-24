# sqs-consumer

Uma CLI desenvolvida em Go para consumir mensagens de uma fila SQS (otimizada para uso local com o LocalStack). Utiliza um **worker pool** com long polling para processar mensagens em paralelo e persisti-las em um arquivo **JSONL**, preservando todas as propriedades (body, atributos de sistema e message attributes) para fins de registro ou reenvio futuro.

## Funcionalidades

- Consumo paralelo e eficiente através de N workers.
- Escrita *thread-safe* em arquivos JSONL.
- Encerramento gracioso (*graceful shutdown*) assegurando que mensagens sendo processadas não sejam perdidas.
- Arquitetura modular e testável.

## Requisitos

- [Go](https://go.dev/) 1.22 ou superior
- [Docker](https://www.docker.com/) (para rodar o LocalStack)
- AWS CLI (`aws-cli`) configurada com o utilitário [`awslocal`](https://github.com/localstack/awscli-local) (opcional, porém recomendado para gerenciar as filas locais).
- [mise](https://mise.jdx.dev/) (opcional, recomendado para padronização de ambiente e execução de tarefas).

## Estrutura do Projeto

O projeto adota a estrutura base de pacotes de mercado (padrão `project-layout`):
- `cmd/sqs-consumer`: Ponto de entrada da aplicação.
- `internal/cli`: Configurações de comandos, parâmetros (Cobra) e injeção de dependências.
- `internal/consumer`: Domínio responsável por regras de polling, mapeamento e exclusão das mensagens no SQS.
- `internal/storage`: Domínio responsável pelos modelos de dados e pela garantia de I/O na escrita concorrente no disco (JSONLWriter).

## Compilação

Você pode compilar o projeto da forma tradicional ou utilizando o gerenciador de tarefas `mise`.

### Forma tradicional (Go CLI)

```bash
# Baixar dependências
go mod tidy

# Gerar o binário na pasta bin/
go build -o bin/sqs-consumer ./cmd/sqs-consumer
```

### Usando o `mise` (Recomendado)

O projeto possui um arquivo `mise.toml` que instala automaticamente a versão exata do Go e embute atalhos rápidos.

```bash
# Instala a versão correta do Go no seu ambiente local isolado
mise install

# Compila o binário na pasta bin/ (o mesmo que go build...)
mise run build

# Para verificar outros atalhos disponíveis (como rodar testes)
mise tasks
```

## Como rodar

O uso básico exige apenas a URL da fila.

```bash
# Consome mensagens e as remove da fila
./bin/sqs-consumer -q http://localhost:4566/000000000000/minha-fila

# Apenas realiza uma cópia local (sem remoção da fila), utilizando 8 workers e salvando em um arquivo customizado
./bin/sqs-consumer -q http://localhost:4566/000000000000/minha-fila -w 8 -o dump.jsonl --no-delete

# Esvazia a fila totalmente e então encerra a aplicação
./bin/sqs-consumer -q http://localhost:4566/000000000000/minha-fila --exit-on-empty
```

### Configurando o ambiente local (LocalStack)

Para simular o SQS na sua máquina com o Docker, você pode rodar o LocalStack:

```bash
docker run --rm -d -p 4566:4566 -e SERVICES=sqs localstack/localstack
```

Em seguida, crie uma fila e adicione uma mensagem de teste utilizando o `awslocal` ou o AWS CLI convencional apontando para o endpoint do LocalStack:

```bash
# Criando a fila
awslocal sqs create-queue --queue-name minha-fila

# Enviando uma mensagem
awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/minha-fila \
  --message-body '{"pedidoId":123}' \
  --message-attributes '{"tipo":{"DataType":"String","StringValue":"pedido"}}'
```
