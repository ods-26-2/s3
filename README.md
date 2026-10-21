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

Há exemplos completos de cada mensagem em [`jsons/`](jsons/).

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
- A idempotência dos comandos depende do repositório: com a persistência em memória, ela se perde quando o processo reinicia.
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
| `src/s3/repository.py` | Alertas, histórico e `message_id` processados (em memória) |
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
| [`MATRIZ_CONTRATOS_S3.md`](docs/MATRIZ_CONTRATOS_S3.md) | Contratos, produtores, consumidores, campos, erros e situação de conformidade |
| [`CHECKLIST_INTEGRACAO_S3.md`](docs/CHECKLIST_INTEGRACAO_S3.md) | Verificações com S2, A3, A5, A7, B1, B2 e B3; deduplicação e ordem |
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

Os testes **não acessam o broker real**: usam assinante, publicador e cliente Kafka falsos. O resultado esperado é `231 passed`.

## Executar com o Kafka

```powershell
$env:PYTHONPATH = "src"
$env:S3_KAFKA_BOOTSTRAP = "<host:porta do broker>"   # ver Guia do Kafka da TRA-1
$env:S3_KAFKA_GROUP_ID = "S3-alertas"                # opcional; este é o padrão
python -m s3 --routing jsons/routing_a5.json --versao-routing routing-a5-1 --tempo-limite 60
```

- O endereço do broker **não fica no código**. O broker de testes está na nuvem e vai mudar para um local.
- Para cada mensagem:
  - o commit do offset só acontece depois do processamento;
  - se o processamento falhar, a mensagem volta na próxima leitura.
- O broker de testes é **pago e compartilhado** pelos 15 squads (Guia do Kafka, 4.5). Por isso `python -m s3` exige `--tempo-limite` ou `--max-mensagens`. Não deixe o processo rodando sem supervisão.
- Por padrão, os tópicos têm o mesmo nome do schema. `PublicadorKafka(topicos={...})` permite usar outra nomenclatura sem mudar o resto do código.
- **Ainda não foi testado contra o broker real.**

## Limitações conhecidas

- **Persistência em memória.** Alertas, histórico e `message_id` processados se perdem quando o processo reinicia. A escolha do banco é decisão da squad: cada build tem o seu, e o P3 opera o armazenamento.
- **Falha ao publicar.** Se o barramento falhar ao publicar `ods.s3.alert`, o alerta já ficou salvo, a falha vai para o log e a mensagem se perde. A reply do comando sai normalmente, de modo que a reentrega não cria um segundo alerta. A solução (fila de saída no banco) depende da persistência.
- **Paginação.** `next_from_at` tem milissegundos. A página termina antes do milissegundo do primeiro alerta que ficou de fora, para não repetir alertas. Se houver mais de `limit` alertas no mesmo milissegundo, a paginação não avança (pendência 15).
- **Alerta criado por comando sem `session_id`.** Regra aprovada em 21/10/2026 (pendência 5), mas ainda não escrita na especificação nem no validador de B1. `captured_at` recebe o horário de criação, porque não há sinal de origem.
- **`clip_id`.** Vale o primeiro clipe do `event_id`. Se o clipe chegar antes do alerta, fica guardado e entra na criação; até 1000 clipes pendentes, porque o S4 grava todo evento, inclusive os que não viram alerta. Ao vincular, o alerta é republicado com o mesmo `state` (pendência 4).
- **Entrega das notificações.** A arquitetura atribui os canais ao S3, mas não há contrato de entrega física. Hoje o S3 só publica `channels` no alerta: não toca som nem envia push (ver pendência 10).
- **Texto do alerta.** `title` e `message` usam os valores do primeiro evento recebido; as fases `UPDATED` não os atualizam.
- **`MUTE`.** Quando o prazo `mute_until` vence, o alerta continua `MUTED`.
- **Concorrência.** O serviço supõe um consumidor por processo.

## Pendências com outras equipes

Itens da revisão da especificação (seção 10, item 17), em que o VER-3 é revisor:

| # | Pendência | Com quem |
|---|---|---|
| 1 | Nome dos tópicos: a especificação usa `ods.s3.alert`; o Guia do Kafka sugere `ods.alerta.emitido` | TRA-1 |
| 2 | Campos de horário do envelope: o guia usa `occurred_at`; a especificação usa `published_at`/`captured_at` | TRA-1 |
| 3 | Tópico das replies e como elas chegam a quem pediu | TRA-1 |
| 4 | ~~Como o S3 obtém o `clip_id`.~~ **Decidido em 21/10/2026: o S3 recebe `ods.s4.clip`** e preenche o `clip_id` quando a rota tem `capture: true`. Falta: a especificação incluir o S3 entre os consumidores de `ods.s4.clip` (seção 9) e aceitar a republicação do alerta quando o clipe é vinculado (a seção 7 só fala em republicar a cada mudança de `state`) | TRA-1, MOD-2 |
| 5 | ~~`session_id` em alerta criado por comando.~~ **Aprovado em 21/10/2026: `session_id` opcional** só em `ods.s3.alert` criado por comando; `sequence` continua obrigatório e conta só pelo produtor; `correlation_id` = `message_id` do comando; `captured_at` = criação. O alerta vindo do S2 continua exigindo `session_id`. Falta: escrever a exceção na especificação e no validador de B1 | TRA-1 (B1), TRA-2 (B2) |
| 6 | Nas fases `UPDATED` e `ENDED`: atualizar a mensagem? Encerrar o alerta quando a violação termina? E se o ENDED chegar antes do STARTED? Hoje o STARTED que chega depois cria um alerta para uma violação já terminada (`test_ended_antes_de_started`) | VER-6, A5 |
| 7 | Transições permitidas e o que acontece quando `mute_until` vence | A5, B6 |
| 8 | Autorização de pessoa: qual `actor` (`user:<id>`) pode mandar ACKNOWLEDGE, MUTE e CLOSE. É decisão do backend de cada aplicação. A conferência do `producer` está na pendência 19 | A5, A3, A7 |
| 9 | Códigos de erro que não estão na especificação: `INVALID_PAYLOAD` e `INVALID_MESSAGE` | TRA-1 |
| 10 | Quem executa e interrompe a entrega física de `SOUND` e `PUSH` (inclusive em ACK, MUTE, CLOSE e no vencimento de `mute_until`) | TRA-1, A5 |
| 11 | Campo `muted_by` no alerta, para o registro de quem desligou (RF6 do A5) | TRA-1, A5 |
| 12 | `ods.session.started`: a seção 7 diz que a camada 3 reinicia o estado a cada sessão, e o contrato fala em "todo componente com estado", mas a lista fechada da seção 9 não inclui o S3. O S3 deve consumir? Se consumir, alertas abertos **não** podem sumir num SEEK de replay | TRA-2, TRA-1 |
| 13 | `ods.artifact.published`: quem usa um artefato deve recarregá-lo ao receber o anúncio. O S3 só carrega o ROUTING de arquivo, na partida, e não há contrato para buscar o conteúdo pela `uri` (`registry://…`) | TRA-1, MOD-1 (B5) |
| 14 | A7 é dono de um artefato ROUTING (seção 8), e a especificação descreve as notificações do A7 como "`ods.s3.alert.command` com o artefato ROUTING". Mas o ROUTING é aplicado a `ods.s2.event`, e a build do A7 não consome o S2. Para que serve o ROUTING do A7? | VER-4, TRA-1 |

| 15 | A consulta pagina só por `next_from_at` (milissegundos). Mais de `limit` alertas no mesmo milissegundo não têm como ser paginados. Basta um desempate (por exemplo, um cursor opaco)? | TRA-1 |
| 16 | Pontos que a especificação não define: `mute_until` no passado, `from_at` maior que `to_at`, `captured_at` de alerta criado por comando e `{duration_s}` (o modelo do ROUTING usa o campo, mas o evento só tem `duration_ms`). Hoje o S3 aceita os dois primeiros, usa o horário de criação no terceiro e calcula `duration_ms // 1000` no quarto | TRA-1, A5 |
| 17 | **Deduplicação por `alert_id`:** a regra 7 da seção 2 chama `alert_id` de "chave de deduplicação", mas o S3 republica o alerta com o mesmo `alert_id` a cada mudança de estado (com `message_id` novo). Pergunta: o consumidor descarta pelo `message_id` e **atualiza** pelo `alert_id`? Se descartar pelo `alert_id`, perde ACK, MUTE e CLOSE. O mesmo vale para `event_id` em `ods.s2.event`. Detalhes no [checklist](docs/CHECKLIST_INTEGRACAO_S3.md), seção 2 | TRA-1 (B1) |
| 18 | **Ordem de entrega:** sem chave de partição, mensagens do mesmo alerta podem chegar fora de ordem. O S3 já publica `ods.s3.alert` com chave `alert_id` (só transporte, sem campo novo); falta o B3 confirmar, e o S2 usar `event_id` como chave em `ods.s2.event`. A outra alternativa é o consumidor ordenar por `sequence`. A segunda não funciona para alertas criados por comando (pendência 5) nem depois de um reinício do S3 | TRA-1 (B3), VER-6 |
| 19 | ~~Produtor fora da lista do contrato.~~ **Decidido em 21/10/2026: quem bloqueia é o B3**, com ACL no broker. O S3 não confere o `producer` dos comandos e não ganha código de erro novo. Falta: o B3 configurar as permissões por tópico | TRA-1 (B3) |

Além do item 17 da seção 10 (S3, revisores VER-3 e VER-4), o VER-3 também revisa os itens 1, 3, 15, 16 e 20. O item 20 inclui o formato do ROUTING.

O estado completo do componente está em [README_S3_ODS.md](docs/README_S3_ODS.md).
