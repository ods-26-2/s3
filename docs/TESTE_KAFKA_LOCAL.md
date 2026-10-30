# Teste do S3 com Kafka local (Docker)

Roda o S3 contra um Kafka de verdade na sua máquina, sem tocar no broker
da TRA-1 (pago e compartilhado). Prepara as entregas S3.2 (barramento real)
e S3.6 (confiabilidade) antes da janela de testes.

## O que precisa

- Docker Desktop rodando ("Engine running").
- O `.venv` do projeto com `confluent-kafka` instalado.

Se o terminal do VS Code disser que `docker` não existe, feche e abra o
VS Code (ele foi aberto antes da instalação e não tem o PATH novo).

## Passo a passo (PowerShell, na raiz do repositório)

```powershell
# 1. Sobe o Kafka (a primeira vez baixa a imagem, ~600 MB)
docker compose -f infra/kafka-local/docker-compose.yml up -d

# 2. Fluxo S2 → S3 → A5
.venv\Scripts\python tools/teste_kafka_local.py

# 3. O mesmo fluxo, mais a queda do broker e a do S3
.venv\Scripts\python tools/teste_kafka_local.py --queda

# 4. Quando terminar
docker compose -f infra/kafka-local/docker-compose.yml down
```

O roteiro só aceita `localhost`/`127.0.0.1`. A cada execução ele apaga e
recria os tópicos do S3 no Kafka local, sobe o `python -m s3` como processo
separado (SQLite novo, `routing_a5.json`, `permissoes_a5.json`) e faz o
papel do S2, do S4 e do A5. Cada mensagem recebida é conferida com
`tests/s3/contrato.py`. Os logs do S3 ficam na pasta mostrada no fim, com
uma linha por mensagem que entrou ou saiu.

## O que é verificado

| Cenário | Verificação |
|---|---|
| Fluxo | evento do S2 → `ods.s3.alert` CREATED, com chave de partição = alert_id, session_id e artifact_version |
| Clipe | `ods.s4.clip` → alerta republicado com o clip_id, mesmo state |
| Comandos | ACK (maria), MUTE negado a ana (UNAUTHORIZED_ACTOR), MUTE e CLOSE (maria); respostas ligadas pelo correlation_id |
| Ordem | CREATED → CREATED (clipe) → ACKNOWLEDGED → MUTED → CLOSED, com sequence crescente |
| Histórico | `ods.s3.alerts.query` devolve o alerta CLOSED, com clipe e autores |
| Queda do broker (`--queda`) | broker parado 10 s; o S3 não cai e reconecta sozinho quando ele volta |
| Queda do S3 (`--queda`) | processo morto; o que chega enquanto ele está fora é processado na volta, e um evento repetido não cria outro alerta |
| Duplicação | nenhum `ods.s3.alert` repetido pelo message_id |

Resultado em 30/10/2026: todos os cenários passaram, com e sem `--queda`.

## O que este teste não cobre

- **Broker caindo entre gravar o alerta e publicá-lo.** Não dá para provocar
  esse instante de fora sem uma corrida de tempo. A fila de saída (outbox)
  nesse caso é coberta por `tests/s3/test_integracao_local.py`
  (`test_queda_do_broker_e_recuperacao`) e por `test_persistencia.py`.
- **O broker oficial.** Endereço, nomes de tópicos, partições, ACLs e
  autenticação da TRA-1 continuam pendentes. Aqui os tópicos têm os nomes
  dos schemas (decisão provisória) e uma partição cada.
- **As aplicações reais.** O S2, o S4 e o A5 são simulados pelo roteiro.
