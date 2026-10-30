# S3 — Alertas e Notificações

Componente S3 da arquitetura 6c da ODS 2026/2, do squad **VER-3 (EPI)**.

O S3 leva o evento qualificado à pessoa certa, pelo canal certo, e registra o que aconteceu depois: quem reconheceu, quem silenciou e quem encerrou o alerta. Ele não tem regra de EPI no código. O que cada tipo de evento gera vem do artefato **ROUTING** da aplicação, por isso o mesmo S3 atende A5 (EPI), A3 e A7.

Os contratos seguem a **Especificação de interfaces 1.0 (arquitetura 6c)** (`ods-6cfinal.html`, publicada pela TRA-1; não fica no repositório). O transporte segue o **Guia de Uso do Kafka** da TRA-1.

## Onde o S3 está

```
I1 → I9 → S2 (VER-6) ──ods.s2.event──► S3 ──ods.s3.alert──► A3, A5, A7 (A1, A2 opcionais)
                                        ▲
        A3/A5/A7 ── artefato ROUTING ───┤
        aplicações ── comandos ─────────┤──► reply
        aplicações ── consulta ─────────┘──► reply
```

| Direção | Schema | Perfil | Quem |
|---|---|---|---|
| Entrada | `ods.s2.event` | EV-SERVIÇO | S2 publica |
| Entrada | artefato `ROUTING` | — | A3, A5 e A7 são donos |
| Entrada | `ods.s4.clip` | EV-SERVIÇO | S4 publica; o S3 usa o `clip_id` quando a rota tem `capture: true` (decisão de 21/10/2026) |
| Saída | `ods.s3.alert` | EV-SERVIÇO | Publicado na criação e a cada mudança de estado |
| Comando | `ods.s3.alert.command` → `.reply` | CMD / RPL | A1, A2, A3, A5 e A7 criam alertas de negócio |
| Comando | `ods.s3.alert_action.command` → `.reply` | CMD / RPL | ACKNOWLEDGE, MUTE e CLOSE |
| Consulta | `ods.s3.alerts.query` → `ods.s3.alerts.reply` | CMD / RPL | Histórico com filtros e paginação |

Quem envia cada comando, pela especificação (seção 9): `alert.command` e `alerts.query` vêm de A1, A2, A3, A5 e A7; `alert_action.command` vem só dos backends de A3, A5 e A7. O S3 consome S2, o artefato ROUTING e, desde 21/10/2026, `ods.s4.clip` (pendência 4). Não consome `ods.session.started`.

Há exemplos completos de cada mensagem em [`contratos/`](contratos/), organizados por equipe (Aplicações, Barramentos, S2, S4), com a explicação em [`contratos/CONTRATOS_S3.md`](contratos/CONTRATOS_S3.md). Os testes usam os exemplos oficiais de `tests/s3/dados/`.

## Ciclo de vida do alerta

| Estado de destino | Pode vir de | Comando |
|---|---|---|
| `CREATED` | — (criação) | evento roteado ou `alert.command` |
| `ACKNOWLEDGED` | `CREATED` | `ACKNOWLEDGE` |
| `MUTED` | `CREATED`, `ACKNOWLEDGED` | `MUTE` (exige `mute_until`) |
| `CLOSED` | `CREATED`, `ACKNOWLEDGED`, `MUTED` | `CLOSE` (estado final) |

- A especificação define os estados, mas **não as transições**. Esta tabela é decisão do VER-3 e ainda precisa ser revisada (ver pendências).
- Transição fora da tabela → reply `ERROR` com `INVALID_TRANSITION`, sem alterar nada.
- Cada ação gera um registro de histórico imutável, com autor (`actor`, no formato `user:<id>`), motivo (`reason`) e horário com fuso.
- O alerta publicado mostra quem reconheceu e encerrou e até quando está silenciado. O contrato não tem `muted_by`: o autor do MUTE fica só no histórico interno.
- A idempotência dos comandos sobrevive ao reinício: a reply de cada `message_id` fica guardada no SQLite.
- O alerta publicado traz `acknowledged_by/at`, `muted_until` e `closed_by/at`.

## Regras de mensagens

- **Envelope comum de 13 campos.** Horários em ISO 8601 UTC com milissegundos (`2026-09-23T14:29:59.870Z`). `schema_version` é `MAJOR.MINOR` e aceita qualquer `1.<n>`.
- **Identificadores são UUID v4** (seção 2, regra 3): `message_id` e `session_id` no envelope; `event_id`, `alert_id` e `clip_id` no payload. Fora do formato: `INVALID_MESSAGE` (envelope) ou `INVALID_PAYLOAD` (payload).
- **Mensagem inválida** (JSON quebrado, envelope ou payload fora do contrato) é **descartada e registrada em log**, nunca processada "na tentativa".
- **Todo comando recebe reply** (seção 2, regra 6), inclusive com envelope inválido (`INVALID_MESSAGE`). Ficam sem reply só a mensagem de outro schema, a de outro `target` e a sem `message_id` UUID, porque não há a quem responder.
- **`ods.s2.event` só é aceito do produtor `S2`** (seção 9, lista fechada).
- **Duplicatas:**
  - mensagem com `message_id` já processado é descartada;
  - o mesmo `event_id` gera um alerta só. Se o alerta já existe, as fases `UPDATED` e `ENDED` não o alteram;
  - sem alerta existente, `UPDATED` cria (o `STARTED` se perdeu e a violação continua) e `ENDED` não cria (a violação já terminou);
  - o fim da violação (`ENDED`) **não** encerra o alerta: o encerramento é ação humana (`CLOSE`);
  - comando repetido (mesmo `message_id`) recebe **a mesma reply**, sem novo efeito.
- **Roteamento:**
  - tipo de evento sem rota não gera alerta e não é erro;
  - evento `UNCERTAIN` só vira alerta se a rota tiver `min_status: UNCERTAIN`;
  - `title` e `message` da rota são modelos com `{campo}`, preenchidos com os campos do evento (`{equipment_item}`, `{zone_id}`, `{duration_s}`…).
- **Destinatários:** `user:<id>`, `group:<id>` ou `role:<id>`.
- **Canais:** `SOUND`, `SCREEN`, `PUSH`.
- **Severidades:** `HIGH`, `MEDIUM`, `LOW`.

## Estrutura

| Arquivo | Responsabilidade |
|---|---|
| `src/s3/envelope.py` | Envelope comum: montar, validar, horários, `sequence` |
| `src/s3/errors.py` | `ErroS3` com o código que vai em `error.code` |
| `src/s3/contracts.py` | `EventoQualificado` e a leitura de `ods.s2.event` |
| `src/s3/routing.py` | Artefato ROUTING, rotas e preenchimento de `title`/`message` |
| `src/s3/alert.py` | Modelo `Alerta` e validação de destinatários, canais e severidade |
| `src/s3/history.py` | `RegistroHistorico` imutável |
| `src/s3/repository.py` | Alertas, histórico, `message_id` processados, `sequence` e fila de saída, com transações (versão em memória, usada nos testes) |
| `src/s3/repository_sqlite.py` | Mesmas regras, gravadas em SQLite: sobrevive ao reinício (usada pelo `python -m s3`) |
| `src/s3/service.py` | Criação, deduplicação, transições e consulta |
| `src/s3/output.py` | Monta `ods.s3.alert` |
| `src/s3/ingestion.py` | Recebe `ods.s2.event` e `ods.s4.clip` |
| `src/s3/commands.py` | Atende os comandos e a consulta e monta as replies |
| `src/s3/subscription.py` / `publication.py` | Interfaces com o barramento e descarte com log |
| `src/s3/kafka_adapter.py` | Adaptador Kafka (`confluent-kafka`) |
| `src/s3/__main__.py` | Executa o S3 ligado ao Kafka |
| `demo_s3.py` | Demonstração com assinante e publicador falsos |
| `tests/s3/` | Testes (`fabricas.py` monta mensagens válidas) |
| `tests/s3/contrato.py` | Verificador escrito a partir da especificação, independente do código do S3 |
| [`contratos/CONTRATOS_S3.md`](contratos/CONTRATOS_S3.md) | **Página dos contratos do S3**, com os nomes da arquitetura. Os exemplos JSON e o PDF de cada equipe ficam em `contratos/Aplicações/`, `contratos/Barramentos/` (nome do schema), `contratos/S2/` e `contratos/S4/` |
| [`MATRIZ_CONTRATOS_S3.md`](docs/MATRIZ_CONTRATOS_S3.md) | Contratos, produtores, consumidores, campos, erros e situação de conformidade |
| [`CHECKLIST_INTEGRACAO_S3.md`](docs/CHECKLIST_INTEGRACAO_S3.md) | Verificações com S2, A3, A5, A7, B1, B2 e B3; deduplicação e ordem |
| [`DECISOES_S3.md`](docs/DECISOES_S3.md) | Decisão tomada em cada pendência, com a origem e com quem confirmar |
| [`REVISAO_ITEM17_S3.md`](docs/REVISAO_ITEM17_S3.md) | Perguntas e propostas do VER-3 para TRA-1 e TRA-2 (revisão do item 17) |

## Instalar e testar (PowerShell)

Requer Python 3.10 ou mais novo. O núcleo usa só a biblioteca padrão.

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt     # pytest e confluent-kafka

$env:PYTHONPATH = "src"
python -m pytest tests/s3 -q            # ou: python -m unittest discover -s tests/s3 -p "test_*.py"
python demo_s3.py
```

Os testes **não acessam o broker real**: usam assinante, publicador e cliente Kafka falsos. O resultado esperado é `357 passed`. Os testes do repositório, do serviço, dos comandos, dos contratos e dos clipes rodam duas vezes: com o repositório em memória e com o SQLite (`test_persistencia.py`).

## Executar com o Kafka

```powershell
$env:PYTHONPATH = "src"
$env:S3_KAFKA_BOOTSTRAP = "<host:porta do broker>"   # ver Guia do Kafka da TRA-1
$env:S3_KAFKA_GROUP_ID = "S3-alertas"                # opcional; este é o padrão
python -m s3 --routing contratos/Barramentos/ROUTING.json --versao-routing routing-a5-1 --banco s3.sqlite3 --tempo-limite 60

# Build sem S2 (como a do A7): sem --routing, só comandos e consultas
python -m s3 --tempo-limite 60
```

- **Persistência:** alertas, histórico, `message_id` processados, `sequence` e a fila de saída ficam no SQLite de `--banco` (padrão: `s3.sqlite3`, fora do git).
- **Fila de saída (transactional outbox):** cada mudança de alerta e a mensagem `ods.s3.alert` correspondente são gravadas na mesma transação. A publicação vem depois: na hora, na partida seguinte e a cada volta do laço do consumidor. Se o Kafka falhar, a mensagem espera na fila, na ordem. A entrega é **pelo menos uma vez**: uma queda entre publicar e marcar como enviada reenvia a mesma mensagem, com o mesmo `message_id`, que o consumidor descarta.
- **Confirmação de entrega:** a publicação só conta como feita quando o relatório de entrega do Kafka (`on_delivery`) vem sem erro. Esvaziar a fila (`flush`) não basta.
- O endereço do broker **não fica no código**. O broker de testes está na nuvem e vai mudar para um local.
- Para cada mensagem:
  - o commit do offset só acontece depois do processamento;
  - se o processamento falhar, a mensagem volta na próxima leitura.
- O broker de testes é **pago e compartilhado** pelos 15 squads (Guia do Kafka, 4.5). Por isso `python -m s3` exige `--tempo-limite` ou `--max-mensagens`. Não deixe o processo rodando sem supervisão.
- Por padrão, os tópicos têm o mesmo nome do schema. `PublicadorKafka(topicos={...})` permite usar outra nomenclatura sem mudar o resto do código.
- **Ainda não foi testado contra o broker real.**

## Limitações conhecidas

- **Persistência.** Um processo por arquivo de banco. A leitura é feita da memória, carregada do banco na partida: serve para o volume de uma build, mas o histórico inteiro fica em memória.
- **Respostas dos comandos** não passam pela fila de saída: se a publicação da reply falhar, o offset não é confirmado, o comando volta e recebe a mesma reply guardada.
- **Paginação.** `next_from_at` tem milissegundos. A página termina antes do milissegundo do primeiro alerta que ficou de fora, para não repetir alertas. Se houver mais de `limit` alertas no mesmo milissegundo, a paginação não avança (pendência 15).
- **Alerta criado por comando sem `session_id`.** Regra aprovada em 21/10/2026 (pendência 5), mas ainda não escrita na especificação nem no validador de B1. `captured_at` recebe o horário de criação, porque não há sinal de origem.
- **`clip_id`.** Vale o primeiro clipe do `event_id`. Se o clipe chegar antes do alerta, fica guardado e entra na criação; até 1000 clipes pendentes, porque o S4 grava todo evento, inclusive os que não viram alerta. Ao vincular, o alerta é republicado com o mesmo `state` (pendência 4).
- **Entrega das notificações.** A arquitetura atribui os canais ao S3, mas não há contrato de entrega física. Hoje o S3 só publica `channels` no alerta: não toca som nem envia push (ver pendência 10).
- **Texto do alerta.** `title` e `message` usam os valores do primeiro evento recebido; as fases `UPDATED` não os atualizam.
- **`MUTE`.** Quando o prazo `mute_until` vence, o alerta continua `MUTED`.
- **Concorrência.** O serviço supõe um consumidor por processo.

## Pendências com outras equipes

Em 21/10/2026 todas as pendências receberam uma decisão. Três foram aprovadas pelas outras equipes; as demais são regras do VER-3 (dono do S3 e do A5) ou decisões provisórias, a confirmar na fase de testes. A tabela completa, com origem e com quem confirmar, está em [docs/DECISOES_S3.md](docs/DECISOES_S3.md).

| Situação | Pendências |
|---|---|
| **Aprovada** (falta escrever na especificação) | 4 (`clip_id` via `ods.s4.clip`), 5 (`session_id` opcional em alerta de comando), 19 (B3 bloqueia remetente) |
| **Do VER-3** (avisar as outras equipes) | 4a (republicar ao vincular o clipe), 6 (UPDATED/ENDED), 7 (transições e `mute_until`), 8 (`actor`), 9 (códigos de erro), 10 (som e push no A5), 11 (desligamento manual = CLOSE), 15 (paginação), 16 (pontos não definidos) |
| **Provisória** (confirmar na fase de testes) | 1 (nome dos tópicos), 2 (horários do envelope), 3 (tópico das replies), 10 (som e push em A3/A7), 12 (`ods.session.started`), 13 (nova versão do ROUTING), 14 (A7 sem ROUTING), 17 (deduplicação), 18 (ordem de entrega) |

Além do item 17 da seção 10 (S3, revisores VER-3 e VER-4), o VER-3 também revisa os itens 1, 3, 15, 16 e 20. O item 20 inclui o formato do ROUTING.

O estado completo do componente está em [README_S3_ODS.md](docs/README_S3_ODS.md).
