# S3 — Alertas e Notificações (Squad VER-3): estado do componente

*Atualizado em 16/10/2026, depois da conferência campo a campo com a Especificação de interfaces 1.0 (`ods-6cfinal.html`).*

## Resumo

O S3 segue os contratos oficiais da **Especificação de interfaces 1.0 (arquitetura 6c)**:
- recebe `ods.s2.event`;
- aplica o artefato ROUTING;
- publica `ods.s3.alert` na criação e a cada mudança de estado;
- atende os comandos `ods.s3.alert.command` e `ods.s3.alert_action.command` e a consulta `ods.s3.alerts.query`, com reply e idempotência pelo `message_id`.

Também existe um adaptador Kafka que segue o Guia de Uso do Kafka da TRA-1.

**Ainda não é uma integração operacional comprovada.** O adaptador Kafka nunca foi executado contra o broker real, e as decisões provisórias de contrato serão confirmadas na fase de testes. Desde 21/10/2026, alertas, histórico, deduplicação e fila de saída ficam em SQLite.

## O que mudou nesta versão

| Antes (contrato provisório) | Agora (Especificação 1.0) |
|---|---|
| Entrada com campos em português (`evento_id`, `tipo`, `timestamp`), com ou sem envelope | Só `ods.s2.event` com o envelope comum. O formato antigo é rejeitado |
| Severidade `ALTA`, canais `SOM/TELA/PUSH`, destinatários livres | `HIGH/MEDIUM/LOW`, `SOUND/SCREEN/PUSH`, `user:`/`group:`/`role:` |
| Roteamento montado no código | Artefato ROUTING carregado de arquivo, com `min_status`, `title`/`message` e `capture` |
| Estados `CRIADO`, `RECONHECIDO`, `SILENCIADO`, `ENCERRADO` | `CREATED`, `ACKNOWLEDGED`, `MUTED` (com `mute_until`), `CLOSED` |
| Saída sem autor e com horários em segundos | `acknowledged_by/at`, `muted_until`, `closed_by/at`; horários com milissegundos |
| Comandos só como funções Python | Mensagens com reply `OK`/`ERROR` e códigos (`ALERT_NOT_FOUND`, `INVALID_TRANSITION`, `INVALID_RECIPIENT`, `INVALID_CHANNEL`) |
| Sem consulta | `ods.s3.alerts.query` com filtros e paginação |
| Sem transporte | `kafka_adapter.py` e `python -m s3` |

### Conferência com a especificação (16/10/2026)

- **Bate com o contrato:** envelope e perfis; campos e tipos de `ods.s2.event`, `ods.s3.alert`, dos dois comandos e da consulta; códigos `ALERT_NOT_FOUND`, `INVALID_TRANSITION`, `INVALID_RECIPIENT` e `INVALID_CHANNEL`; formato do ROUTING; `correlation_id` (`event_id` ou `message_id` do comando criador); `limit ≤ 500`; reply `ods.s3.alerts.reply`.
- **Corrigido nesta revisão:**
  - os identificadores passaram a ser validados como UUID v4: `message_id` e `session_id` no envelope; `event_id`, `alert_id` e `clip_id` no payload;
  - `schema_version` agora precisa ser `MAJOR.MINOR` (antes, `"1.x"` ou `"1"` passavam).
- **Sprint 3, etapa 1 (revisão dos contratos):**
  - uma falha ao publicar `ods.s3.alert` não é mais repassada; antes, a reentrega de um `alert.command` criava um segundo alerta;
  - um comando com envelope inválido agora recebe reply `ERROR` (`INVALID_MESSAGE`); antes, era descartado sem resposta;
  - `ods.s2.event` só é aceito do produtor `S2`;
  - a paginação da consulta não repete alertas criados no mesmo milissegundo;
  - um alerta criado por comando sai sem `session_id` (nenhum valor é inventado); em 21/10/2026 isso passou a ser a regra proposta pelo VER-3 (pendência 5);
  - novo `tests/s3/contrato.py` confere as mensagens publicadas e os exemplos de `tests/s3/dados/` com regras escritas a partir da especificação.
- **Não implementado, por depender de outras equipes:** consumo de `ods.session.started` e de `ods.artifact.published`, e conferência do `producer` dos comandos (pendências 8, 12 e 13 do README.md).

Os nomes internos (`Alerta`, `EventoQualificado`, `evento_id`…) continuam em português. A tradução para os nomes do contrato fica só na borda: `contracts.py`, `output.py` e `commands.py`.

## Bugs corrigidos

1. **Destinatário sem vírgula virava lista de letras.** `("supervisor-01")` não é uma tupla, é texto, e virava `['s','u','p',…]`. Agora o campo precisa ser uma lista ou tupla, e cada destinatário precisa ter um prefixo válido.
2. **O estado do alerta podia mudar sem passar pelo histórico.** O repositório agora devolve cópias. O estado só muda por `atualizar()`, que grava estado e histórico juntos e não altera nada se algo for inválido.
3. **Validações fracas:**
   - `timestamp` aceitava qualquer texto; agora todo horário precisa ser ISO 8601 com fuso;
   - autor feito só de espaços era aceito; agora é rejeitado;
   - severidade, estado e canal não eram conferidos; agora são.
4. **`ENDED` criava alerta.** Um `ENDED` de evento sem alerta existente criava um alerta novo para uma violação já terminada. Agora não cria, e o caso é registrado em log.
5. **`actor` sem formato.** O `actor` dos comandos aceitava qualquer texto. Agora precisa ser `user:<id>`.
6. **Teste duplicado.** `test_reconhecer_alerta` tinha duas definições e uma apagava a outra (corrigido na revisão anterior).

## Testes

```powershell
$env:PYTHONPATH = "src"
python -m pytest tests/s3 -q
```

**Resultado:** 357 testes e 155 subtestes passando. O alerta criado por comando segue a regra aprovada de `session_id` opcional (pendência 5), conferida em `tests/s3/contrato.py` com `origem="comando"`.

O que os testes cobrem:
- envelope e horários;
- leitura de `ods.s2.event`, incluindo o exemplo `tests/s3/dados/entrada.json`;
- roteamento, incluindo o artefato `tests/s3/dados/routing_a5.json`;
- ciclo de vida e histórico;
- deduplicação por `message_id` e por `event_id`;
- saída igual a `tests/s3/dados/saida.json`;
- os três comandos, incluindo os arquivos de exemplo;
- descarte de mensagem inválida com log;
- adaptador Kafka com cliente falso.

O que os testes **não** cobrem: broker real, persistência durável, envio real por som, tela e push, e integração ponta a ponta com o S2 e as aplicações.

## Pendências

### Internas (não dependem de outras equipes)
1. ~~**Persistência em banco.**~~ Feita em 21/10/2026: SQLite com fila de saída (transactional outbox). Alertas, histórico, deduplicação e `sequence` sobrevivem ao reinício, e uma falha de publicação não perde mais a mensagem.
2. **Teste manual curto contra o broker de testes,** combinado com a TRA-1.
3. ~~**Organização do repositório.**~~ Feita em 21/10/2026: documentação em `docs/`, `.git copy/`, `LICENSE copy` e a especificação fora do git.
4. **`Analise_Projeto_S3_ODS.pdf` está desatualizado** em relação a este documento.

### Com outras equipes
Em 21/10/2026 cada pendência recebeu uma decisão (aprovada, do VER-3 ou provisória): ver [DECISOES_S3.md](DECISOES_S3.md). As provisórias são confirmadas na fase de testes.

As 19 perguntas estão na seção "Pendências com outras equipes" do [README.md](../README.md). As mais urgentes:
- o nome dos tópicos (TRA-1);
- como o S3 obtém o `clip_id` (TRA-1 e MOD-2);
- o comportamento nas fases `UPDATED` e `ENDED` (VER-6 e A5);
- escrever na especificação as decisões de 21/10/2026: `session_id` opcional em alerta de comando, S3 consumindo `ods.s4.clip` e bloqueio de produtor pelo B3 (TRA-1);
- deduplicação por `alert_id` e ordem de entrega, que afetam todos os consumidores do S3 (TRA-1; pendências 17 e 18).

Contratos, produtores e situação de cada um: [MATRIZ_CONTRATOS_S3.md](MATRIZ_CONTRATOS_S3.md). Roteiro de integração: [CHECKLIST_INTEGRACAO_S3.md](CHECKLIST_INTEGRACAO_S3.md).

## Critérios de aceite

- [x] Uma mensagem `ods.s2.event` válida gera exatamente um alerta.
- [x] Mensagens e eventos repetidos não duplicam alertas nem histórico.
- [x] Comandos registram autor, motivo e horário, e respondem com o contrato oficial.
- [x] O encerramento segue transições documentadas.
- [ ] ACK, MUTE e CLOSE só são aceitos de origem autorizada (pendência 8).
- [x] Alertas e histórico sobrevivem ao reinício (persistência SQLite, `test_persistencia.py`).
- [ ] A saída publicada é consumida por um componente real, através do broker real.
- [ ] Testes de integração ponta a ponta S2 → S3 → A5.
