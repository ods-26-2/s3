# Decisões do S3 — Alertas e Notificações (VER-3)

*Atualizado em 21/10/2026. Os números seguem as pendências do [README](../README.md).*

Cada decisão tem uma origem:

- **Aprovada**: outra equipe respondeu em 21/10/2026. Falta só escrever na especificação.
- **Do VER-3**: a regra é do VER-3, porque o squad é dono dos contratos do S3 e da aplicação A5 ("cada squad é dono dos contratos dos seus componentes", seção 1). Vale até a squad mudar; basta avisar as outras equipes.
- **Provisória**: depende de outra equipe. O S3 segue a nossa recomendação **até a confirmação na fase de testes**. Não deve ser apresentada como regra oficial.

| # | Tema | Decisão | Origem | Confirmar na fase de testes com |
|---|---|---|---|---|
| 1 | Nome dos tópicos | Um tópico por schema, com o nome do schema (`ods.s3.alert`, `ods.s3.alert.command`…). O `PublicadorKafka(topicos={...})` permite trocar sem mudar o código | Provisória | TRA-1 (B3) |
| 2 | Horários do envelope | `published_at` e `captured_at`, da especificação; não o `occurred_at` do Guia do Kafka | Provisória | TRA-1 |
| 3 | Respostas dos comandos | Tópico `ods.s3.<nome>.reply`; quem pediu acha a sua pelo `correlation_id` | Provisória | TRA-1 (B3) |
| 4 | `clip_id` | O S3 consome `ods.s4.clip` e preenche o `clip_id` quando a rota tem `capture: true` | Aprovada | TRA-1: incluir o S3 como consumidor de `ods.s4.clip` (seção 9) |
| 4a | Republicar ao vincular o clipe | Vale o primeiro clipe do evento; o alerta é republicado com o mesmo `state`. Se o clipe chegar antes, entra na criação | Do VER-3 | TRA-1: a seção 7 só fala em republicar a cada mudança de `state` |
| 5 | `session_id` em alerta de comando | Opcional só em `ods.s3.alert` criado por comando; `sequence` conta só pelo produtor; `correlation_id` = `message_id` do comando; `captured_at` = criação | Aprovada | TRA-1 (B1): exceção no perfil EV-SERVIÇO e no validador |
| 6 | Fases UPDATED e ENDED | Com alerta existente, nenhuma fase o altera. ENDED **não** encerra o alerta (o fim da violação não é o fim do atendimento). ENDED sem alerta não cria. Se o ENDED chegar antes do STARTED, o STARTED ainda cria o alerta: a violação aconteceu e precisa de registro | Do VER-3 (A5) | Avisar o VER-6 |
| 7 | Transições e `mute_until` | Tabela atual: ACK só de CREATED; MUTE de CREATED ou ACKNOWLEDGED; CLOSE de qualquer estado menos CLOSED. Quando `mute_until` vence, o alerta continua MUTED: o S3 não tem temporizador, e a aplicação usa o `muted_until` para voltar a avisar. Depois de MUTED, só CLOSE | Do VER-3 (A5) | Avisar A3 e A7 |
| 8 | Quem pode agir (`actor`) | O backend de cada aplicação confere se a pessoa pode reconhecer, silenciar ou encerrar antes de mandar o comando. O S3 só registra quem fez | Do VER-3 | Avisar A3 e A7 |
| 9 | Códigos de erro | `INVALID_PAYLOAD` (payload fora do contrato) e `INVALID_MESSAGE` (envelope fora do contrato) passam a ser códigos do S3, ao lado dos quatro da especificação | Do VER-3 (dono da tabela de erros do S3) | Avisar a TRA-1 para constar na seção 7 |
| 10 | Entrega de SOUND e PUSH | O S3 só publica `channels`. O backend de cada aplicação toca o som e envia o push, para quando chega ACK, MUTE ou CLOSE e volta a avisar quando o `muted_until` passa | Do VER-3 (A5); provisória para A3 e A7 | TRA-1, A3, A7 |
| 11 | Desligamento manual (RF6) e `muted_by` | Desligar o alarme = **CLOSE**, porque `closed_by` e `closed_at` aparecem no alerta. MUTE é pausa temporária. Não pedir `muted_by` agora: o backend do A5 já sabe quem silenciou, porque é ele que manda o comando, e o S3 guarda no histórico | Do VER-3 (A5) | — |
| 12 | `ods.session.started` | O S3 não consome; segue a lista fechada da seção 9. O histórico de alertas não zera em sessão nova | Provisória | TRA-2 (B2) |
| 13 | Nova versão do ROUTING | Carregada de arquivo na partida, pela versão fixada na build (seção 8). Versão nova = reiniciar o S3 com o arquivo novo. `ods.artifact.published` fica para depois | Provisória | TRA-1, MOD-1 (B5) |
| 14 | ROUTING do A7 | O A7 cria alertas só por comando. O S3 roda **sem** `--routing` e então assina só os tópicos de comando | Provisória | VER-4 |
| 15 | Paginação da consulta | `next_from_at` é inclusivo. A página termina antes do milissegundo do `next_from_at`. Mais de `limit` alertas no mesmo milissegundo é limitação conhecida | Do VER-3 (contrato do S3) | — |
| 16 | Pontos que a especificação não define | `{duration_s}` = `duration_ms ÷ 1000`, arredondado para baixo. `mute_until` no passado é aceito, porque os relógios podem divergir e o replay usa horários passados. `from_at` maior que `to_at` devolve lista vazia. `captured_at` de alerta de comando = criação | Do VER-3 (contrato do S3) | — |
| 17 | Deduplicação | O consumidor descarta pelo `message_id` e **atualiza** o alerta pelo `alert_id`. O S3 não muda | Provisória | TRA-1 (B1): texto da regra 7; avisar A3, A5 e A7 |
| 18 | Ordem de entrega | `ods.s3.alert` com chave de partição `alert_id` (já implementado). O consumidor ignora `sequence` menor para o mesmo alerta. O S2 deve usar `event_id` como chave | Provisória | TRA-1 (B3), VER-6 |
| 19 | Remetente fora da lista | O B3 bloqueia no broker; o S3 não confere o `producer` dos comandos | Aprovada | TRA-1 (B3): configurar as permissões |

## Respostas ao envio dos contratos (até 30/10/2026)

Os contratos foram enviados às aplicações, ao barramento e às camadas de serviço.

| Equipe | Resposta | O que muda no S3 |
|---|---|---|
| **A5** (VER-3) | Contratos ok | Nada. As decisões "Do VER-3" sobre o A5 (6, 7, 10 e 11) ficam confirmadas |
| **A2** (VER-2) | Não vai usar o S3 | Nada no código. A2 era remetente possível de `alert.command` e `alerts.query` e consumidor opcional de `ods.s3.alert`; sai das verificações de integração |
| **S4** (MOD-2) | Campos alinhados com a 6c; `event_id` é o mesmo do S2 (`null` em captura manual); o clipe sai **depois** do alerta, porque o S4 só publica após gravar o MP4; `media_uri` é referência lógica (`s4://…`), resolvida pelo S4 | Confirma o desenho atual: o S3 vincula o clipe numa republicação e não precisa do `clip_id` na criação. O S3 não guarda `media_uri`: o alerta leva só `clip_id`, e a aplicação pede o vídeo ao S4 |
| Demais (A3, A7, S2, TRA-1, TRA-2) | Sem resposta | — |

**Exemplo real do S4 (29/10/2026) passado pelo S3:** é rejeitado. Com as quatro correções abaixo, é aceito.

| Problema na mensagem do S4 | Regra | O S4 já sabia? |
|---|---|---|
| Falta `captured_at` | Perfil EV-SERVIÇO (seção 3) | Não |
| Falta `sequence` | Perfil EV-SERVIÇO (seção 3) | Não |
| `session_id` = `exec-demo-1`, não é UUID | Seção 2, regra 3 | Não |
| `clip_id` com prefixo `clip-`, não é UUID | Seção 2, regra 3 | Sim, está nas pendências do S4 |

Recomendação ao S4, sem bloquear o S3: `correlation_id` = `event_id` na captura automática (linha de `ods.s4.clip`, seção 7).

## Fora dos contratos
- **Persistência (SQLite) e fila de saída**, interna ao S3 e sem mudança de contrato (21/10/2026):
  - **Problema:** um reinício apagava alertas, histórico e deduplicação e zerava o `sequence`. Uma falha ao publicar perdia a mensagem. Uma queda entre criar o alerta e guardar a reply do comando fazia a reentrega criar um segundo alerta.
  - **Solução:** `SqliteAlertRepository`, com as mesmas regras do repositório em memória, e *transactional outbox*. A mudança de estado, a mensagem a publicar e o `message_id` recebido são gravados na mesma transação. Um despachante publica a fila na ordem, para na primeira falha e tenta de novo na partida e a cada volta do consumidor.
  - **Garantia:** entrega pelo menos uma vez, com o mesmo `message_id` no reenvio. Consumidores descartam repetidos pelo `message_id` (seção 2, regra 7).
  - **Confirmação de entrega do Kafka:** só conta o relatório `on_delivery` sem erro.
  - **Contratos afetados:** nenhum. Só melhora o cumprimento da idempotência (regra 6), da deduplicação (regra 7) e do `sequence` (seção 3).
- **Teste real com S2 e aplicações:** em 21/10/2026, eles ainda não conseguem mandar e receber mensagens. As confirmações desta tabela entram nesse teste. O roteiro está em [CHECKLIST_INTEGRACAO_S3.md](CHECKLIST_INTEGRACAO_S3.md).
