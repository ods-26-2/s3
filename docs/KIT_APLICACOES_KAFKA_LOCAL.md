# Kit: testar o backend da aplicação contra o S3 (Kafka local)

Para o backend do A5 (e, quando quiserem, A3 e A7) testar contra o S3 de
verdade, sem o broker da TRA-1 e sem esperar o S2: o kit sobe o S3 num
Kafka local e faz o papel do S2 e do S4.

Contratos: `contratos/Aplicações/` (o que a aplicação manda e recebe) e
`contratos/CONTRATOS_S3.md`. Regras provisórias: `docs/DECISOES_S3.md`.

## 1. Preparar (uma vez)

- Docker Desktop rodando, repositório do S3 e o `.venv` com `confluent-kafka`.
- O backend roda **na máquina** (fora do Docker) e conecta em `localhost:9092`.
  O Kafka local anuncia `localhost`: um backend dentro de outro contêiner não
  consegue conectar sem mudar o `docker-compose.yml`.

```powershell
docker compose -f infra/kafka-local/docker-compose.yml up -d
.venv\Scripts\python tools/kit_aplicacoes.py preparar --zerar   # tópicos limpos
```

## 2. Rodar o S3 (deixe um terminal aberto)

```powershell
.venv\Scripts\python tools/kit_aplicacoes.py s3
```

Usa `tests/s3/dados/routing_a5.json` (PPE_NON_COMPLIANT → HIGH, SCREEN e PUSH,
com captura) e `tests/s3/dados/permissoes_a5.json`:

| Ação | Quem pode |
|---|---|
| ACKNOWLEDGE | `role:supervisor` (user:maria, user:joao) e `role:tecnico-seguranca` (user:ana) |
| MUTE, CLOSE | só `role:supervisor` |

Opções: `--routing <arquivo>`, `--permissoes <arquivo>` (o de vocês),
`--sem-routing` (como na build do A7: só comandos), `--banco <arquivo>`
(padrão `s3-local.sqlite3`; apague para começar sem histórico).

## 3. Gerar alertas (outro terminal)

```powershell
.venv\Scripts\python tools/kit_aplicacoes.py evento                 # S2: STARTED CONFIRMED, event_id novo
.venv\Scripts\python tools/kit_aplicacoes.py clipe <event_id>       # S4: clipe do evento
.venv\Scripts\python tools/kit_aplicacoes.py evento --status UNCERTAIN  # não vira alerta (min_status CONFIRMED)
```

O S3 publica o alerta em `ods.s3.alert`; com o clipe, republica o mesmo
alerta, no mesmo `state`, com o `clip_id`.

## 4. O que o backend faz

| Direção | Tópico | Observação |
|---|---|---|
| Lê | `ods.s3.alert` | chave de partição = `alert_id` |
| Manda | `ods.s3.alert_action.command` | ACKNOWLEDGE, MUTE (com `mute_until`), CLOSE; `actor` = `user:<id>` |
| Manda | `ods.s3.alert.command` | alerta de negócio, sem passar pelo ROUTING |
| Manda | `ods.s3.alerts.query` | histórico, com filtros e paginação por `next_from_at` |
| Lê | `ods.s3.alert_action.reply`, `ods.s3.alert.reply`, `ods.s3.alerts.reply` | a resposta é a que tem `correlation_id` = `message_id` do pedido |

Nome do tópico = nome do schema, e respostas em `.reply` filtradas pelo
`correlation_id`: decisões provisórias, a confirmar com a TRA-1.

Regras que o backend precisa seguir (DECISOES_S3.md):

- **Repetição × atualização (17):** descarta mensagem com `message_id` já
  visto; mensagem nova com o mesmo `alert_id` **atualiza** o alerta, não cria outro.
- **Ordem (18):** guarda o último `sequence` aplicado de cada `alert_id` e
  ignora um menor.
- **Clipe (4):** o `clip_id` pode chegar depois, numa republicação com o mesmo `state`.
- **Som e push (10):** quem toca o som e manda o push é o backend, pelos
  `channels` do alerta. Para quando chega ACK, MUTE ou CLOSE, e volta a
  avisar quando o `muted_until` passa (o S3 deixa o alerta em MUTED).
- **Transições (7):** ACK só de CREATED; MUTE de CREATED ou ACKNOWLEDGED;
  CLOSE de qualquer estado menos CLOSED. Fora disso: `INVALID_TRANSITION`.
- **Erros na resposta:** `ALERT_NOT_FOUND`, `INVALID_TRANSITION`,
  `UNAUTHORIZED_ACTOR` (actor sem permissão), `INVALID_PAYLOAD`, `INVALID_MESSAGE`.

## 5. Cenários de consumo (D1–D5)

Publicam direto em `ods.s3.alert` versões de alerta geradas pelo código do
S3, duplicadas ou fora de ordem, e mostram o resultado esperado no backend.
Não precisam do S3 rodando.

```powershell
.venv\Scripts\python tools/kit_aplicacoes.py cenario        # todos
.venv\Scripts\python tools/kit_aplicacoes.py cenario D4     # um só
```

| # | O que chega | Esperado no backend |
|---|---|---|
| D1 | o mesmo CREATED duas vezes (mesmo `message_id`) | um alerta; a segunda é descartada |
| D2 | CREATED e ACKNOWLEDGED do mesmo `alert_id` | um alerta, em ACKNOWLEDGED |
| D3 | CREATED → ACKNOWLEDGED → MUTED → CLOSED | CLOSED, com `acknowledged_by`, `muted_until` e `closed_by` |
| D4 | CREATED, CLOSED, depois ACKNOWLEDGED e MUTED (sequence menor) | continua CLOSED |
| D5 | alerta criado por comando, sem `session_id` e sem `event_id` | aceito |

## 6. Roteiro sugerido com o S3 rodando

1. `evento` → o alerta aparece na tela (SCREEN) e o push sai (PUSH).
2. `clipe <event_id>` → o alerta ganha o vídeo, sem duplicar.
3. ACK como `user:ana` → sucesso; MUTE como `user:ana` → `UNAUTHORIZED_ACTOR`.
4. MUTE e CLOSE como `user:maria` → o som e o push param.
5. Consulta ao histórico → o alerta vem CLOSED, com os autores.
6. Feche o S3 (Ctrl+C), mande um comando, abra o S3 de novo → a resposta
   chega na volta, sem perder nada.

## Limites

O S2 e o S4 são simulados. Nomes de tópicos, ACLs e autenticação do broker
oficial ainda dependem da TRA-1.
