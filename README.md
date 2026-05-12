[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/BK9AX0KL)

# Exercício 11 – RabbitMQ: Sistema de Processamento de Pedidos de E-commerce

Sistema distribuído de processamento de pedidos que demonstra múltiplos produtores,
múltiplas filas distintas e consumidores com finalidades de negócio específicas,
usando **RabbitMQ/AMQP** com a biblioteca `rabbitpy`.

---

## Domínio da Aplicação

A aplicação simula o fluxo completo de um pedido em uma loja virtual:

```
[Loja Virtual]          → fila pedidos.novos      → [Gerente de Estoque]
                                                            ↓
                                                    fila pedidos.aprovados
                                                            ↓
                                                     [Central de Logística]
                                                            ↓
                                                    fila pedidos.enviados
                                                            ↓
                                                    [Monitor de Rastreio]

[Gateway de Pagamento]  → fila pagamentos.pendentes → [Processador Financeiro]
```

---

## Arquitetura de Componentes

### Exchange

| Nome   | Tipo   | Descrição                                           |
|--------|--------|-----------------------------------------------------|
| `loja` | direct | Exchange único; roteamento por routing key = nome da fila |

### Filas

| Fila                    | Produzida por              | Consumida por              | Conteúdo                                            |
|-------------------------|----------------------------|----------------------------|-----------------------------------------------------|
| `pedidos.novos`         | `producer_loja.py`         | `consumer_estoque.py`      | Pedido novo: cliente, produto, quantidade, valor    |
| `pagamentos.pendentes`  | `producer_financeiro.py`   | `consumer_pagamento.py`    | Confirmação de pagamento: método, valor, aprovação  |
| `pedidos.aprovados`     | `consumer_estoque.py`      | `consumer_logistica.py`    | Pedido com estoque reservado, pronto para despacho  |
| `pedidos.enviados`      | `consumer_logistica.py`    | `consumer_rastreio.py`     | Pedido despachado com código de rastreio e NF       |

### Produtores

| Arquivo                   | Função de negócio                                                       |
|---------------------------|-------------------------------------------------------------------------|
| `producer_loja.py`        | Simula clientes comprando na loja; gera pedidos aleatórios com produto, quantidade e cliente |
| `producer_financeiro.py`  | Simula retorno do gateway de pagamento (webhook); aprova ~85 % dos pagamentos |

### Consumidores

| Arquivo                   | Fila consumida          | Tarefa simulada                                                        |
|---------------------------|-------------------------|------------------------------------------------------------------------|
| `consumer_estoque.py`     | `pedidos.novos`         | Verifica inventário, reserva unidades, encaminha para logística        |
| `consumer_pagamento.py`   | `pagamentos.pendentes`  | Calcula taxa da operadora, registra crédito ou rejeição com motivo     |
| `consumer_logistica.py`   | `pedidos.aprovados`     | Escolhe transportadora, calcula frete, gera rastreio e emite NF        |
| `consumer_rastreio.py`    | `pedidos.enviados`      | Registra envio, simula próximo evento de status, exibe painel ao vivo  |

> **Nota:** `consumer_estoque.py` e `consumer_logistica.py` são simultaneamente
> consumidores e produtores, formando um **pipeline de mensagens** — padrão
> comum em sistemas de integração com RabbitMQ.

---

## Pré-requisitos

### Broker (servidor)

```bash
# Instalar RabbitMQ (Ubuntu/Debian)
sudo bash install_rabbitmq.sh
sudo systemctl start rabbitmq-server

# Criar usuário, vhost e permissões
sudo rabbitmqctl add_user myuser abc123
sudo rabbitmqctl add_vhost my_vhost
sudo rabbitmqctl set_permissions -p my_vhost myuser ".*" ".*" ".*"
```

Abrir portas no firewall: **5671–5672**

### Cliente Python

```bash
pip install rabbitpy
```

Edite `const.py` e ajuste `RABBITMQ_ADDR` para o IP do servidor.

---

## Como Executar (6 terminais)

```bash
# Terminal 1 – Loja virtual (produtor de pedidos)
python3 producer_loja.py

# Terminal 2 – Gateway de pagamento (produtor de confirmações)
python3 producer_financeiro.py

# Terminal 3 – Gerente de estoque (consumidor/produtor)
python3 consumer_estoque.py

# Terminal 4 – Processador financeiro (consumidor)
python3 consumer_pagamento.py

# Terminal 5 – Central de logística (consumidor/produtor)
python3 consumer_logistica.py

# Terminal 6 – Monitor de rastreio (consumidor)
python3 consumer_rastreio.py
```

Os produtores podem ser iniciados em qualquer ordem; os consumidores
devem estar ativos para que o pipeline flua end-to-end.

---

## Fluxo de Dados Detalhado

1. **`producer_loja.py`** gera pedidos a cada 2–5 s com produto, cliente e valor
   aleatórios e os publica em `pedidos.novos`.

2. **`consumer_estoque.py`** lê cada pedido, consulta o inventário em memória e:
   - Se houver estoque → decrementa, marca `status_estoque = RESERVADO` e publica
     em `pedidos.aprovados`.
   - Se não → descarta com log de recusa.

3. **`consumer_logistica.py`** lê os pedidos aprovados, seleciona transportadora
   (com base no peso), gera código de rastreio, calcula frete, emite NF e publica
   em `pedidos.enviados`.

4. **`consumer_rastreio.py`** lê os despachos, registra no histórico e simula o
   próximo evento de status (COLETADO → EM_TRANSITO → … → ENTREGUE), exibindo um
   painel atualizado a cada mensagem.

5. **`producer_financeiro.py`** publica confirmações de pagamento (independentes do
   fluxo de pedidos) em `pagamentos.pendentes`, simulando o callback assíncrono de
   um gateway externo.

6. **`consumer_pagamento.py`** processa cada transação: calcula a taxa da operadora
   (varia por método: PIX 0 %, débito 1,5 %, crédito 3 %) e mantém o resumo
   financeiro acumulado.

---

## Comparação: RabbitMQ/AMQP × Kafka

### Diferenças Fundamentais de Modelo

| Dimensão              | RabbitMQ / AMQP                              | Apache Kafka                                    |
|-----------------------|----------------------------------------------|-------------------------------------------------|
| **Abstração central** | Message broker (fila de tarefas)             | Log de eventos distribuído                      |
| **Modelo de entrega** | Push: o broker empurra mensagens ao consumidor | Pull: o consumidor lê do log no próprio ritmo |
| **Persistência**      | Mensagem removida após ACK                   | Mensagem retida pelo período configurado (padrão 7 dias) |
| **Reprocessamento**   | Não nativo (requer DLX + requeue)            | Nativo: basta resetar o offset do consumer group |
| **Roteamento**        | Flexível: direct, topic, fanout, headers     | Fixo: por tópico e partição                     |
| **Ordenação**         | FIFO por fila                                | Garantida apenas dentro de uma mesma partição   |
| **Consumer groups**   | Não nativo (competing consumers por fila)    | Nativo e central ao modelo                      |
| **Throughput**        | ~50–100 k msg/s (com persistência)           | Milhões de msg/s (batching + zero-copy)         |
| **Latência**          | Muito baixa (< 1 ms)                         | Baixa a moderada (5–20 ms típico)               |

### Aplicabilidade ao Sistema Implementado

#### Por que RabbitMQ foi a escolha natural para este sistema

| Característica do sistema              | Vantagem do RabbitMQ                                                        |
|----------------------------------------|-----------------------------------------------------------------------------|
| Cada pedido deve ser processado **uma única vez** | Semântica de ACK/NACK garante "exactly-once processing" por consumidor |
| Filas com **propósitos distintos**     | Exchange direct com routing keys mapeia exatamente ao modelo AMQP           |
| Pipeline com **etapas dependentes**    | Consumers/producers encadeados (estoque → logística → rastreio) são naturais |
| Erros de estoque ou pagamento          | Dead-Letter Exchange (DLX) e NACK com requeue permitem retry granular       |
| Complexidade de roteamento moderada    | Sem necessidade de particionamento ou consumer groups                       |

#### Quando Kafka seria preferível neste domínio

| Cenário                                    | Razão para Kafka                                                            |
|--------------------------------------------|-----------------------------------------------------------------------------|
| **Auditoria completa** de todos os pedidos | Replay do log permite reprocessar eventos históricos sem backup especial    |
| **Analytics em tempo real** (vendas, estoque, tendências) | Consumers de BI e ML leem o mesmo stream sem interferir na operação |
| **Escala massiva** (milhões de pedidos/dia) | Particionamento horizontal distribui carga sem bottleneck no broker         |
| **Event sourcing** do estado do pedido     | O log Kafka é a fonte de verdade; o estado é derivado dos eventos           |
| Múltiplos sistemas consumindo **o mesmo evento** independentemente | Cada consumer group tem seu próprio offset; zero coordenação |

### Resumo da Comparação

```
RabbitMQ é ideal quando:              Kafka é ideal quando:
  • As mensagens são tarefas            • As mensagens são eventos
    one-shot (processe e descarte)        que precisam ser reprocessados
  • O roteamento é complexo e           • O throughput é muito alto
    variado (topic/fanout/headers)        (> 100 k msg/s)
  • A latência ultra-baixa              • Múltiplos consumidores
    é crítica                             independentes leem o mesmo stream
  • O sistema precisa de RPC            • É necessário histórico auditável
    ou request-reply                      dos eventos (event sourcing)
  • Workflows com etapas               • Analytics e streaming analytics
    dependentes e ACK granular            (Kafka Streams, ksqlDB)
```

Para este exercício — um sistema de pedidos com pipeline sequencial, filas
específicas por responsabilidade e semântica clara de "processar e confirmar" —
**RabbitMQ/AMQP é a tecnologia mais adequada**. Kafka seria superdimensionado
para o volume e padrão de acesso aqui representados, mas seria a escolha correta
se o requisito fosse, por exemplo, alimentar um data warehouse com o histórico
completo de todas as transações da loja.

---

## Referências

- RabbitMQ Documentation: https://www.rabbitmq.com/docs/
- AMQP 0-9-1 Model Explained: https://www.rabbitmq.com/tutorials/amqp-concepts
- rabbitpy library: https://github.com/gmr/rabbitpy
- Tanenbaum & van Steen, *Distributed Systems*, 4th ed., 2025
