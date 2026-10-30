# Contratos do S3 — Alertas e Notificações (VER-3)

*Especificação de interfaces 1.0, arquitetura 6c. Estado do código: branch `feature/contratos`, commit `34e7575`, 21/10/2026.*

Os nomes são os da arquitetura. Cada contrato tem um exemplo em `Barramentos/`, com o mesmo nome do schema (`ods.s3.alert.json`…), e os mesmos exemplos com nomes em português em `Aplicações/`, `S2/` e `S4/`. Os exemplos foram gerados rodando o S3 e conferidos com `tests/s3/contrato.py`.

**Marcas:**
- **[Oficial]**: está na especificação.
- **[Aprovada]**: decidida com as equipes em 21/10/2026; falta escrever na especificação.
- **[Do VER-3]**: regra do S3 ou do A5.
- **[Provisória]**: falta a outra equipe confirmar.

Detalhes de cada decisão: [DECISOES_S3.md](../docs/DECISOES_S3.md).

## Mapa: o que entra e o que sai

| Contrato | Modo | S3 | Quem envia ou recebe do outro lado | Exemplo |
|---|---|---|---|---|
| [`ods.s2.event`](#odss2event) | evento | **recebe** | S2 publica | [ods.s2.event.json](Barramentos/Evento%20recebido%20do%20S2.json) |
| [`ods.s4.clip`](#odss4clip) | evento | **recebe** [Aprovada] | S4 publica | [ods.s4.clip.json](Barramentos/Evid%C3%AAncia%20do%20S4.json) |
| [`ROUTING`](#routing) | artefato | **recebe** | A3, A5, A7 entregam (arquivo) | [ROUTING.json](Barramentos/ROUTING.json) |
| [`ods.s3.alert.command`](#odss3alertcommand--reply) | comando | **recebe** | A1, A2, A3, A5, A7 enviam | [ods.s3.alert.command.json](Barramentos/ods.s3.alert.command.json) |
| [`ods.s3.alert.reply`](#odss3alertcommand--reply) | resposta | **envia** | volta para quem pediu | [ods.s3.alert.reply.json](Barramentos/ods.s3.alert.reply.json) |
| [`ods.s3.alert_action.command`](#odss3alert_actioncommand--reply) | comando | **recebe** | A3, A5, A7 enviam | [ods.s3.alert_action.command.json](Barramentos/ods.s3.alert_action.command.json) |
| [`ods.s3.alert_action.reply`](#odss3alert_actioncommand--reply) | resposta | **envia** | volta para quem pediu | [ods.s3.alert_action.reply.json](Barramentos/ods.s3.alert_action.reply.json) |
| [`ods.s3.alerts.query`](#odss3alertsquery--reply) | consulta | **recebe** | A1, A2, A3, A5, A7 enviam | [ods.s3.alerts.query.json](Barramentos/ods.s3.alerts.query.json) |
| [`ods.s3.alerts.reply`](#odss3alertsquery--reply) | resposta | **envia** | volta para quem pediu | [ods.s3.alerts.reply.json](Barramentos/ods.s3.alerts.reply.json) |
| [`ods.s3.alert`](#odss3alert) | evento | **envia** | A3, A5, A7 recebem; A1, A2 opcionais | [ods.s3.alert.json](Barramentos/ods.s3.alert.json) |

O S3 **não** consome `ods.session.started` [Provisória] nem `ods.artifact.published` [Provisória].

## Regras comuns [Oficial]

- **Envelope de 13 campos:**
  - os seis fixos são `message_type`, `message_id` (UUID v4), `schema`, `schema_version` (MAJOR.MINOR; o S3 aceita qualquer `1.x`), `producer` e `published_at` (ISO 8601 em UTC com milissegundos);
  - os demais dependem do perfil: EV-SERVIÇO para eventos, CMD para comandos e consultas, RPL para respostas.
- **Comandos e consultas:**
  - levam `target = "S3"`;
  - todos recebem resposta;
  - o mesmo `message_id` recebe **a mesma resposta**, sem efeito novo.
- **Respostas:** levam `correlation_id` = `message_id` do pedido e `status` OK ou ERROR; em erro, `error {code, message}`.
- **Deduplicação:** o consumidor descarta `message_id` já visto. O alerta é atualizado pelo `alert_id` [Provisória: texto da regra 7].
- **Envelope inválido [Do VER-3]:** recebe resposta ERROR `INVALID_MESSAGE`, desde que o `message_id` seja UUID. Sem `message_id` válido não há a quem responder.

---

## `ods.s2.event`

**Evento qualificado** · EV-SERVIÇO · produz: **S2** (VER-6) · consome: S3, S4, S5, A3, A5, A8 · exemplo: [ods.s2.event.json](Barramentos/Evento%20recebido%20do%20S2.json)

| Campo | Obrig. | Tipo | Regra no S3 |
|---|---|---|---|
| `event_id` | Sim | UUID | O mesmo em STARTED, UPDATED e ENDED; um alerta por `event_id` |
| `event_type`, `rule_id` | Sim | string | `event_type` decide a rota no ROUTING |
| `status` | Sim | CONFIRMED, UNCERTAIN | UNCERTAIN só vira alerta se a rota tiver `min_status: UNCERTAIN` |
| `phase` | Sim | STARTED, UPDATED, ENDED | Ver regras abaixo |
| `confidence` | Sim | number 0–1 | |
| `started_at` | Sim | datetime | |
| `duration_ms` | Sim | integer ≥ 0 | Vira `{duration_s}` (÷ 1000, para baixo) [Do VER-3] |
| `source_ids` | Sim | [string] | |
| `entity_id`, `class`, `zone_id`, `equipment_item` | Não | string | Usados no texto do alerta |
| `details` | Não | object | Suas chaves também podem ir no texto |

Envelope: `producer` **tem que ser `S2`**; `captured_at`, `session_id` (UUID) e `sequence` são obrigatórios.

Como o S3 trata cada caso [Do VER-3]:
- **Mensagem inválida:** descartada com log, sem resposta, porque é evento.
- **`event_type` sem rota:** ignorado.
- **STARTED ou UPDATED de evento novo:** cria o alerta.
- **Evento que já tem alerta:** nenhuma fase o altera.
- **ENDED sem alerta:** não cria.
- **ENDED antes do STARTED:** o STARTED ainda cria o alerta.
- **ENDED nunca encerra o alerta.**

## `ods.s4.clip`

**Descritor de mídia** · EV-SERVIÇO · produz: **S4** (MOD-2) · consome: A1–A6, A8, opcional A7 e **S3 [Aprovada: falta constar na seção 9]** · exemplo: [ods.s4.clip.json](Barramentos/Evid%C3%AAncia%20do%20S4.json)

| Campo | Obrig. | Tipo | Regra no S3 |
|---|---|---|---|
| `clip_id` | Sim | UUID | Vai para o `clip_id` do alerta |
| `event_id` | Não | UUID | Sem `event_id`, o clipe é ignorado |
| `media_type` | Sim | CLIP, SNAPSHOT | |
| `media_uri`, `hash_sha256` | Sim | string | |
| `start_at`, `end_at`, `expires_at` | Sim | datetime | |

Envelope: `producer` **tem que ser `S4`**.

Regras [Do VER-3]:
- **Só vincula quando a rota tem `capture: true`.**
- **Vale o primeiro clipe** do evento.
- **Clipe que chega antes do alerta** fica guardado (até 1000) e entra na criação.
- **Ao vincular, o alerta é republicado** com o mesmo `state`. Falta a TRA-1 aceitar, porque a seção 7 só fala em mudança de `state`.

## `ROUTING`

**Artefato** · dono: **A3, A5, A7** · consome: S3 · exemplo: [ROUTING.json](Barramentos/ROUTING.json)

| Campo | Obrig. | Tipo | Descrição |
|---|---|---|---|
| `routes[].event_type` | Sim | string | Tipo roteado; uma rota por tipo |
| `routes[].min_status` | Sim | CONFIRMED, UNCERTAIN | UNCERTAIN roteia também os eventos em dúvida |
| `routes[].severity`, `recipients`, `channels` | Sim | — | Como em `ods.s3.alert` |
| `routes[].title`, `message` | Sim | string | Modelos com `{campo}`: `event_id`, `event_type`, `rule_id`, `status`, `entity_id`, `class`, `zone_id`, `equipment_item`, `confidence`, `started_at`, `duration_ms`, `duration_s`, `source_id` e chaves de `details` |
| `routes[].capture` | Sim | boolean | Vincular o `clip_id` do S4 [Aprovada] |

Entrega [Provisória]:
- um arquivo por build (`--routing`), com a versão (`--versao-routing`), que vai em `artifact_version` de cada alerta;
- versão nova = reiniciar o S3;
- rota inválida impede o S3 de subir;
- **A7: sem ROUTING**, o S3 só atende comandos.

## `ods.s3.alert`

**Alerta** · EV-SERVIÇO · produz: **S3** ao criar, a cada mudança de `state` [Oficial] e ao vincular o clipe [Do VER-3] · consome: **A3, A5, A7**; opcionais A1, A2 · exemplo: [ods.s3.alert.json](Barramentos/ods.s3.alert.json)

| Campo | Obrig. | Tipo | Descrição |
|---|---|---|---|
| `alert_id` | Sim | UUID | Identidade do alerta; não muda |
| `event_id` | Não | UUID | Evento de S2; ausente em alerta de negócio |
| `title`, `message` | Sim | string | Texto exibido |
| `severity` | Sim | HIGH, MEDIUM, LOW | |
| `recipients` | Sim | [string] | `user:<id>`, `group:<id>` ou `role:<id>` |
| `channels` | Sim | [SOUND, SCREEN, PUSH] | O S3 só publica; quem entrega é a aplicação [Do VER-3] |
| `clip_id` | Não | UUID | Evidência no S4; pode chegar numa republicação |
| `state` | Sim | CREATED, ACKNOWLEDGED, MUTED, CLOSED | |
| `created_at` | Sim | datetime | |
| `acknowledged_by`, `acknowledged_at` | Não | string, datetime | |
| `muted_until` | Não | datetime | Quando passa, o alerta continua MUTED [Do VER-3] |
| `closed_by`, `closed_at` | Não | string, datetime | |

Envelope:

| Origem do alerta | `correlation_id` | `session_id` | `captured_at` | Outros |
|---|---|---|---|---|
| Evento | `event_id` | o do evento | o do evento | `source_id` do evento; `artifact_version` = versão do ROUTING |
| Comando | `message_id` do comando | **ausente [Aprovada]** | criação | — |

O `sequence` conta por sessão; sem sessão, só pelo produtor. Ele não volta a 0 no reinício. Chave de partição: `alert_id` [Provisória].

## `ods.s3.alert.command` / `.reply`

**Criar alerta** · CMD → RPL · envia: **A1, A2, A3, A5, A7** · atende: S3 · exemplos: [pedido](Barramentos/ods.s3.alert.command.json), [resposta](Barramentos/ods.s3.alert.reply.json)

| Campo | Obrig. | Tipo |
|---|---|---|
| `title`, `message` | Sim | string não vazia |
| `severity` | Sim | HIGH, MEDIUM, LOW |
| `recipients` | Sim | lista não vazia de `user:`/`group:`/`role:<id>` |
| `channels` | Sim | lista não vazia de SOUND, SCREEN, PUSH |
| `event_id`, `clip_id` | Não | UUID |

- **Resposta:** `{ "status": "OK", "alert_id": "…" }`.
- **Erros [Oficial]:** `INVALID_RECIPIENT`, `INVALID_CHANNEL`.
- **Erros [Do VER-3]:** `INVALID_PAYLOAD` (gravidade, campo ausente, UUID) e `INVALID_MESSAGE` (envelope).

## `ods.s3.alert_action.command` / `.reply`

**Reconhecer, silenciar, encerrar** · CMD → RPL · envia: **backends de A3, A5, A7** · atende: S3 · exemplos: [pedido (MUTE)](Barramentos/ods.s3.alert_action.command.json), [resposta](Barramentos/ods.s3.alert_action.reply.json)

| Campo | Obrig. | Tipo |
|---|---|---|
| `alert_id` | Sim | UUID |
| `action` | Sim | ACKNOWLEDGE, MUTE, CLOSE |
| `actor` | Sim | `user:<id>` |
| `mute_until` | Sim em MUTE | datetime (passado é aceito) |
| `reason` | Não | string |

Transições [Do VER-3]:

| Ação | A partir de | Para |
|---|---|---|
| ACKNOWLEDGE | CREATED | ACKNOWLEDGED |
| MUTE | CREATED, ACKNOWLEDGED | MUTED |
| CLOSE | CREATED, ACKNOWLEDGED, MUTED | CLOSED, que é final |

- **Resposta:** `{ "status": "OK", "alert_id": "…", "state": "…" }`.
- **Erros [Oficial]:** `ALERT_NOT_FOUND`, `INVALID_TRANSITION`.
- **Erros [Do VER-3]:** `INVALID_PAYLOAD`, `INVALID_MESSAGE` e `UNAUTHORIZED_ACTOR` (o `actor` não tem permissão para a ação).
- **Desligamento manual do A5 (RF6)** = CLOSE [Do VER-3].
- **Quem pode agir [Do VER-3]:** cada build entrega ao S3 um arquivo de permissões (`--permissoes`) com quem pode dar ACKNOWLEDGE, MUTE e CLOSE, por `user:`, `role:` ou `group:`. Fora dele, a resposta é `UNAUTHORIZED_ACTOR` e nada muda. Modelo: [Aplicações/Permissões de ação (exemplo).json](Aplica%C3%A7%C3%B5es/Permiss%C3%B5es%20de%20a%C3%A7%C3%A3o%20(exemplo).json).

## `ods.s3.alerts.query` / `.reply`

**Histórico de alertas** · CMD → RPL · envia: **A1, A2, A3, A5, A7** · atende: S3 · exemplos: [consulta](Barramentos/ods.s3.alerts.query.json), [resposta](Barramentos/ods.s3.alerts.reply.json)

| Campo da consulta | Obrig. | Tipo | Regra |
|---|---|---|---|
| `from_at` | Não | datetime | Sobre `created_at`; inclusivo |
| `to_at` | Não | datetime | Exclusivo; `from_at` > `to_at` devolve lista vazia |
| `state` | Não | enum do alerta | |
| `severity` | Não | HIGH, MEDIUM, LOW | |
| `limit` | Não | integer 1–500 | Padrão 100 |

**Payload da resposta (o histórico):**

| Campo | Obrig. | Tipo | Descrição |
|---|---|---|---|
| `status` | Sim | OK, ERROR | |
| `alerts` | Sim | array | Cada item é o **payload de `ods.s3.alert`** (tabela acima), no estado atual, em ordem de `created_at` e depois `alert_id` |
| `next_from_at` | Não | datetime | Só quando há mais; repita a consulta com `from_at` = este valor |

- **Paginação [Do VER-3]:** a página termina antes do milissegundo de `next_from_at`, para não repetir alertas. Limitação: mais de `limit` alertas no mesmo milissegundo não avançam.
- **Erros [Do VER-3]:** `INVALID_PAYLOAD` (filtro inválido, `limit` fora de 1–500) e `INVALID_MESSAGE`.
