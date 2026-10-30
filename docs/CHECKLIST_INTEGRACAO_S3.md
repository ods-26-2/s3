# Checklist de integração do S3 (VER-3)

*Referência: Especificação de interfaces 1.0, arquitetura 6c (`ods-6cfinal.html`, publicada pela TRA-1; não fica no repositório). Contratos e situação de cada um: [MATRIZ_CONTRATOS_S3.md](MATRIZ_CONTRATOS_S3.md). Pendências numeradas: [README.md](../README.md).*

Cada item tem uma marca: **[Oficial]** é exigido pela especificação; **[Pendente]** depende de decisão (com o número da pendência); **[Recomendação]** é proposta técnica do VER-3.

As decisões provisórias de [DECISOES_S3.md](DECISOES_S3.md) são confirmadas durante estes testes.

Os testes de integração devem usar o broker de testes **só em janela combinada com a TRA-1**, porque ele é pago e compartilhado. Tudo o que der para provar localmente deve ser provado antes.

## 1. Verificações por equipe

### S2 (VER-6)
- [ ] **[Pendente]** Em 21/10/2026 o S2 e as aplicações **ainda não conseguem** mandar e receber mensagens reais. Até lá, o teste é local, com os JSONs de `jsons/`.
- [ ] **[Oficial]** O S2 publica `ods.s2.event` com envelope EV-SERVIÇO completo e `producer = "S2"`; o S3 rejeita qualquer outro produtor.
- [ ] **[Oficial]** `event_id` é UUID v4 e é o mesmo em STARTED, UPDATED e ENDED.
- [ ] **[Oficial]** Os campos obrigatórios estão presentes, incluindo `duration_ms` inteiro e `source_ids`.
- [ ] **[Oficial]** UNCERTAIN só vira alerta se a rota tiver `min_status: UNCERTAIN`. Demonstrar com uma rota de cada tipo.
- [ ] **[Pendente 6]** Combinar o que o S3 faz em UPDATED e ENDED. Hoje o alerta não muda, e ENDED sem alerta existente não cria nada.
- [ ] **[Pendente 18]** Confirmar a ordem de entrega por `event_id`. Se o ENDED chegar antes do STARTED, o S3 cria um alerta para uma violação já terminada (teste `test_ended_antes_de_started`).
- [ ] **[Recomendação]** Enviar o mesmo `ods.s2.event` duas vezes e conferir que sai um único alerta.

### A3 (VER-6), A5 (VER-3) e A7 (VER-4) — consumidores de `ods.s3.alert`
- [ ] **[Oficial]** O consumidor aceita o mesmo formato de `ods.s3.alert`, sem variação por aplicação.
- [ ] **[Oficial]** O consumidor aceita alerta sem `event_id`, que é um alerta de negócio, e sem `clip_id`.
- [ ] **[Oficial]** A aplicação envia `ods.s3.alert.command` e recebe `ods.s3.alert.reply` com `alert_id`.
- [ ] **[Oficial]** A3, A5 e A7 enviam `alert_action.command` com `actor = user:<id>` e recebem a reply com o `state` novo.
- [ ] **[Oficial]** A consulta `ods.s3.alerts.query` com filtros devolve `alerts[]` no formato de `ods.s3.alert`. Paginar até o fim com `next_from_at`.
- [ ] **[Oficial]** Toda reply tem `correlation_id` igual ao `message_id` do pedido.
- [ ] **[Pendente 17]** Os testes de deduplicação e ordem da seção 2.3.
- [ ] **[Pendente 7]** A5 valida a tabela de transições e o que acontece quando `mute_until` vence.
- [ ] **[Pendente 8]** Cada backend decide quem (`actor`) pode reconhecer, silenciar e encerrar.
- [ ] **[Pendente 10]** Quem toca o som e envia o push. O S3 só publica `channels`.
- [ ] **[Provisória 14]** **A7:** o S3 da build do A7 roda sem `--routing` e só atende comandos. Confirmar que o A7 não precisa de ROUTING.
- [ ] **[Decidido 4]** **A5:** o `clip_id` agora vem no alerta quando a rota tem `capture: true`. Ele pode chegar numa republicação do alerta, com o mesmo `state`, alguns segundos depois da criação.

### B1 (TRA-1) — contratos
- [ ] **[Oficial]** Passar as mensagens publicadas pelo S3 (`ods.s3.alert` e as três replies) no validador de B1, assim que ele for publicado. Hoje usamos `tests/s3/contrato.py`, escrito a partir da especificação.
- [x] **[Decidido 5]** `session_id` opcional em alerta criado por comando: aprovado em 21/10/2026. Falta escrever a exceção na especificação e no validador de B1.
- [ ] **[Pendente 9]** Aprovar ou substituir os códigos provisórios `INVALID_PAYLOAD` e `INVALID_MESSAGE`.
- [ ] **[Pendente 17]** Esclarecer a regra 7 da seção 2: o que significa "`alert_id` é chave de deduplicação".
- [x] **[Decidido 19]** Produtor fora da lista: quem bloqueia é o B3, no broker (21/10/2026).
- [ ] **[Pendente 15, 16]** Paginação da consulta e pontos não definidos.

### B2 (TRA-2) — tempo e sessão
- [x] **[Decidido 5]** Alerta criado por comando não cita sessão (21/10/2026).
- [ ] **[Pendente 12]** O S3 consome `ods.session.started`? Se consumir, alertas abertos não podem ser zerados num SEEK de replay.
- [ ] **[Recomendação]** Conferir a latência (`published_at` − `captured_at`) dos alertas de evento num cenário gravado de P6.

### B3 (TRA-1) — barramento
- [ ] **[Pendente 1]** Confirmar os nomes dos tópicos. A especificação diz "um tópico por schema"; o S3 usa o nome do schema como tópico.
- [ ] **[Pendente 3]** Confirmar o tópico das replies (`ods.s3.<x>.reply`) e o filtro por `correlation_id`.
- [ ] **[Pendente 18]** Garantia de ordem por alerta: chave de partição `alert_id` em `ods.s3.alert` e `event_id` em `ods.s2.event`. Desde 21/10/2026, o `PublicadorKafka` usa o `alert_id` como chave em `ods.s3.alert`; falta o B3 confirmar, e o S2 decidir a chave de `ods.s2.event`.
- [ ] **[Decidido 19, falta executar]** ACL por tópico e credencial por aplicação, para que só A3, A5 e A7 escrevam em `alert_action.command`.
- [ ] **[Recomendação]** Teste curto no broker de testes: um `ods.s2.event` de exemplo, o alerta, um ACK, um CLOSE e uma consulta, com `--tempo-limite`.

## 2. Deduplicação e atualização de estado

### 2.1 O que a especificação diz [Oficial]
- **Seção 2, regra 7:** "todo consumidor descarta mensagens cujo `message_id` já processou. Para eventos de domínio, `event_id` (S2) e `alert_id` (S3) são as chaves de deduplicação."
- **Seção 7:** `ods.s3.alert` é publicado "ao criar e a cada mudança de `state`", ou seja, várias mensagens com o mesmo `alert_id`.
- **Seção 3:** `message_id` é a "identidade desta mensagem"; `sequence` é o "contador crescente por `producer` + `session_id`, a partir de 0; permite detectar perda e ordenar".
- O payload de `ods.s3.alert` **não tem** campo de versão nem `updated_at`. Também não tem `muted_at`.

### 2.2 O que o S3 faz [Código]
Conferido no teste `TestRepublicacaoEOrdem.test_cada_republicacao_tem_message_id_novo`:

| Campo | Na criação | Em cada mudança de estado |
|---|---|---|
| `message_id` | novo | **novo** (UUID v4 gerado em `envelope.montar_envelope`) |
| `alert_id` | novo | **o mesmo** |
| `session_id` | o do evento de origem | o mesmo |
| `sequence` | próximo valor do contador da sessão | **incrementa** (0, 1, 2…) |
| `correlation_id` | `event_id` ou `message_id` do comando criador | o mesmo |
| `published_at` | instante da publicação | instante da publicação; **pode repetir o milissegundo** |

**Conclusão:** o contrato permite separar uma repetição de uma atualização. Repetição tem o mesmo `message_id` e deve ser descartada. Atualização tem `message_id` novo e o mesmo `alert_id`, e deve **substituir** o registro do alerta. O problema é de texto: se a regra 7 for lida como "descartar quando o `alert_id` já foi visto", o consumidor perde ACK, MUTE e CLOSE. A leitura correta, a confirmar com B1, é que `alert_id` é a chave do registro e não de descarte (pendência 17). O `alert_id` não deve mudar: ele identifica o alerta ao longo do ciclo de vida.

### 2.3 Entrega fora de ordem

| Situação | Comprovado no código ou risco de integração | Efeito |
|---|---|---|
| Até 20/10/2026, `PublicadorKafka.publicar` chamava `produce()` **sem chave** | **Corrigido em 21/10/2026**: chave `alert_id` (`test_chave_de_particao`) | Sem chave, mensagens do mesmo alerta iam para partições diferentes e podiam chegar fora de ordem. A ordem de `ods.s2.event` depende da chave usada pelo S2 |
| Consumidor recebe CLOSED antes de ACKNOWLEDGED | Risco de integração | Se aplicar "a última que chegou", o alerta volta a ACKNOWLEDGED. Com `sequence` dá para perceber, porque o ACK tem `sequence` menor na mesma sessão |
| Alerta criado por comando, sem `session_id` | **Comprovado**, pela regra proposta (pendência 5) | O `sequence` conta só pelo produtor (S3). Para ordenar as versões do mesmo alerta, o consumidor usa a chave de partição `alert_id` e o `sequence` |
| O S3 reinicia | **Corrigido em 21/10/2026**: o `sequence` fica no SQLite (`test_sequence_continua_depois_do_reinicio`) | O `sequence` continua de onde parou na mesma sessão |
| `published_at` igual em duas publicações | **Comprovado** (mesmo milissegundo no teste) | `published_at` não serve para desempatar |
| ENDED chega antes do STARTED na entrada | **Comprovado** (`test_ended_antes_de_started`) | O S3 cria um alerta para uma violação que já terminou (pendências 6 e 18) |

**[Recomendação]**, sem mudar o contrato:
1. **B3:** publicar `ods.s3.alert` com chave `alert_id`, e `ods.s2.event` com chave `event_id`. O Kafka garante ordem dentro da partição, então todas as mensagens do mesmo alerta chegam em ordem. É configuração de transporte, sem campo novo. No S3, isso já foi feito para `ods.s3.alert` (`PublicadorKafka.chave`); falta a confirmação do B3 e a decisão do S2.
2. **Consumidores:** guardar, por `alert_id`, o último `(session_id, sequence)` aplicado e ignorar mensagens com `sequence` menor na mesma sessão.
3. **Desempate secundário, que só vale enquanto a tabela de transições do VER-3 não mudar:** todas as transições permitidas sobem de estado, `CREATED < ACKNOWLEDGED < MUTED < CLOSED`, e CLOSED é final. Um consumidor nunca deve sair de CLOSED. Se o A5 aprovar "silenciar de novo" ou "MUTE vence e volta a ACKNOWLEDGED", esta regra deixa de valer.

### 2.4 Testes de integração propostos para A3, A5 e A7
| # | Cenário | Resultado esperado no consumidor |
|---|---|---|
| D1 | A mesma mensagem `ods.s3.alert` entregue duas vezes (mesmo `message_id`) | Processa uma vez |
| D2 | CREATED e depois ACKNOWLEDGED do mesmo `alert_id`, com `message_id` diferentes | Atualiza para ACKNOWLEDGED, sem criar um segundo alerta |
| D3 | CREATED → ACKNOWLEDGED → MUTED → CLOSED | Termina em CLOSED com `acknowledged_by`, `muted_until` e `closed_by` |
| D4 | CLOSED (`sequence` 3) entregue antes de ACKNOWLEDGED (`sequence` 1) | Continua CLOSED |
| D5 | Alerta criado por comando, sem `session_id` | Aceito pelo consumidor; ordenado por `sequence`, que conta só pelo produtor (pendência 5) |
| D6 | O S3 reinicia entre duas publicações do mesmo alerta | O `sequence` continua crescendo; mensagens que não saíram antes da queda saem na partida, na ordem e com o mesmo `message_id` |
| D7 | A consulta `alerts.query` logo depois de um CLOSE | O item consultado tem o mesmo `state` da última `ods.s3.alert` |
| D8 | Alertas de duas sessões diferentes (replay com SEEK) | Cada alerta mantém a ordem pela própria sessão |

D1 a D4 e D7 dá para fazer localmente: um produtor falso entrega os JSONs na ordem escolhida ao consumidor de cada aplicação, sem broker. D6 e D8 dependem do S3 rodando e de P6.
