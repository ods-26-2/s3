# Matriz de contratos do S3 — Alertas e Notificações (VER-3)

*Referência: Especificação de interfaces 1.0, arquitetura 6c ([`ods-6cfinal.html`](ods-6cfinal.html)). Estado do código em 20/10/2026: 208 testes passando e 1 falha esperada (`xfail`).*

Cada afirmação traz uma marca:

- **[Oficial]**: está escrito na especificação (a seção vem entre parênteses).
- **[Código]**: é o que o S3 faz hoje, conferido no código e nos testes.
- **[Pendente]**: depende de decisão de outra equipe; o número remete à lista de pendências do [README.md](README.md).
- **[Recomendação]**: é proposta técnica do VER-3, ainda sem aprovação.

## 1. Situação de cada contrato

| Situação | Significado |
|---|---|
| Conforme nos testes locais | Segue a especificação nos testes, mas ainda não passou pelo broker real nem pelo validador de B1, que ainda não foi publicado |
| Incompatível | Viola a especificação num caso conhecido |
| Depende de decisão externa | A especificação não define o comportamento |
| Não implementado | O contrato existe na arquitetura, mas o S3 não o atende |

Nenhum contrato aparece como "Conforme" sem ressalva: nada foi testado no broker real nem contra o validador oficial de B1.

| Contrato | Tipo | Perfil | Envia / produz [Oficial] | Atende / consome [Oficial] | Valida | Processa | Testes | Situação |
|---|---|---|---|---|---|---|---|---|
| `ods.s2.event` | evento (entrada) | EV-SERVIÇO | S2 | S3, S4, S5, A3, A5, A8 | `envelope.validar_envelope`, `contracts.EventoQualificado.de_mensagem`, `ingestion.IngestorEventos.receber` (exige `producer = S2`) | `service.AlertService.processar_evento_roteado` | `test_contracts`, `test_ingestion`, `test_envelope`, `test_contratos_saida` | Conforme nos testes locais. O que fazer em UPDATED/ENDED depende de decisão externa (pendência 6) |
| ROUTING | artefato (entrada) | — | A3, A5, A7 (donos) | S3 | `routing.RoteadorAlertas.de_artefato`, `routing.ConfiguracaoRoteamento` | `routing.RoteadorAlertas.resolver`, `routing.renderizar` | `test_routing` | Conforme nos testes locais, só a carga por arquivo. `capture` depende de decisão externa (pendência 4); recarga por anúncio não implementada (pendência 13) |
| `ods.s3.alert` (alerta de evento) | evento (saída) | EV-SERVIÇO | S3 | A3, A5, A7; opcionais A1, A2 | `alert.Alerta.validar` | `output.serializar_alerta`, `service.AlertService._publicar` | `test_output`, `test_publication`, `test_contratos_saida` | Conforme nos testes locais |
| `ods.s3.alert` (alerta de comando) | evento (saída) | EV-SERVIÇO | S3 | idem | idem | idem | `test_contratos_saida.TestAlertaDeComando` (`xfail`) | **Incompatível**: sai sem `session_id`. Depende de decisão externa (pendência 5) |
| `ods.s3.alert.command` → `.reply` | comando | CMD → RPL | A1, A2, A3, A5, A7 | S3 | `commands.ProcessadorComandos.receber`, `_criar` | `service.AlertService.criar_alerta` | `test_commands`, `test_contratos_saida` | Conforme nos testes locais. A conferência do produtor depende de decisão externa (seção 3, pendência 19) |
| `ods.s3.alert_action.command` → `.reply` | comando | CMD → RPL | backends de A3, A5, A7 | S3 | `commands.ProcessadorComandos.receber`, `_acao` | `service.AlertService._mudar_estado` | `test_commands`, `test_service`, `test_contratos_saida` | Conforme nos testes locais. Produtor (pendência 19), transições (pendência 7) e autorização (pendência 8) dependem de decisão externa |
| `ods.s3.alerts.query` → `ods.s3.alerts.reply` | consulta | CMD → RPL | A1, A2, A3, A5, A7 | S3 | `commands.ProcessadorComandos._consultar` | `repository.AlertRepository.listar` | `test_commands`, `test_repository`, `test_contratos_saida` | Conforme nos testes locais. Produtor (pendência 19) e desempate da paginação (pendência 15) dependem de decisão externa |
| `ods.session.started` | evento | EV-SINAL | P4, P5, P6 | I2, S1, S2, S4, S5, B4 ("e todo componente com estado") | — | — | — | Não implementado; a especificação se contradiz (pendência 12) |
| `ods.artifact.published` | evento | EV-SERVIÇO | dono do artefato | "consumidores do artefato" | — | — | — | Não implementado; não há contrato para buscar o conteúdo pela `uri` (pendência 13) |
| `ods.s4.clip` | evento | EV-SERVIÇO | S4 | A1–A6, A8; opcional A7. **Não inclui o S3** | — | — | — | Não implementado, por ser lista fechada; por isso `capture` não tem como ser atendido (pendência 4) |

## 2. Campos, respostas e erros

### `ods.s2.event` (entrada)
- **[Oficial] Envelope:** EV-SERVIÇO, isto é, os seis campos fixos mais `captured_at`, `session_id` e `sequence`. `source_id` e `artifact_version` aparecem quando se aplicam (seção 3).
- **[Oficial] Payload obrigatório:** `event_id` (UUID), `event_type`, `rule_id`, `status` (`CONFIRMED`/`UNCERTAIN`), `phase` (`STARTED`/`UPDATED`/`ENDED`), `confidence` (0.0–1.0), `started_at`, `duration_ms` (inteiro), `source_ids`.
- **[Oficial] Payload opcional:** `entity_id`, `class`, `zone_id`, `equipment_item`, `details` (seção 7).
- **[Código] Mensagem inválida:** é descartada com log, sem reply, porque é um evento. A mensagem repetida (mesmo `message_id`) é descartada. Há um alerta por `event_id`.
- **[Código] Regra de negócio, não de formato:** `UNCERTAIN` só vira alerta quando a rota tem `min_status: UNCERTAIN`. Com alerta existente, nenhuma fase o altera. Sem alerta existente, ENDED não cria (pendência 6).

### ROUTING (artefato)
- **[Oficial] Campos:** `routes[]` com `event_type`, `min_status`, `severity`, `recipients`, `channels`, `title`, `message` e `capture`, todos obrigatórios (seção 8).
- **[Oficial] Versão:** a versão do artefato viaja em `artifact_version` de cada alerta que ele produziu (seção 8).
- **[Código] Validação:** o artefato é tratado como versionado. `RoteadorAlertas.de_artefato(conteudo, versao)` rejeita campo ausente, rota repetida e valores inválidos, e a versão vai no envelope.
- **[Código] Carga:** o arquivo vem de `--routing` e a versão de `--versao-routing`. Não há leitura do registro de B5.
- **[Código] `{duration_s}`:** é derivado como `duration_ms // 1000`. A especificação usa o campo no modelo, mas ele não existe no evento (pendência 16).

### `ods.s3.alert` (saída)
- **[Oficial] Envelope:** EV-SERVIÇO. `correlation_id` é o `event_id` ou o `message_id` do comando criador. `artifact_version` é a versão do ROUTING quando ele participou (seções 3 e 7).
- **[Oficial] Payload obrigatório:** `alert_id`, `title`, `message`, `severity` (`HIGH`/`MEDIUM`/`LOW`), `recipients` (`user:`/`group:`/`role:`), `channels` (`SOUND`/`SCREEN`/`PUSH`), `state` (`CREATED`/`ACKNOWLEDGED`/`MUTED`/`CLOSED`), `created_at`.
- **[Oficial] Payload opcional:** `event_id`, `clip_id`, `acknowledged_by`, `acknowledged_at`, `muted_until`, `closed_by`, `closed_at`.
- **[Oficial] Publicação:** na criação e a cada mudança de `state`.
- **[Código] Um formato para todos:** o mesmo formato serve A3, A5 e A7; só os dados mudam conforme o ROUTING de cada aplicação.
- **[Código] Republicação:** cada publicação tem `message_id` novo e `sequence` crescente; o `alert_id` não muda (seção 2 do checklist).

### `ods.s3.alert.command` → `ods.s3.alert.reply`
- **[Oficial] Payload obrigatório:** `title`, `message`, `severity`, `recipients`, `channels`. **Opcional:** `event_id`, `clip_id`.
- **[Oficial] Reply:** `{ "status": "OK", "alert_id": "…" }`. **Erros:** `INVALID_RECIPIENT`, `INVALID_CHANNEL`.
- **[Código] Erros provisórios:** `INVALID_PAYLOAD` para `severity` inválida, `title`/`message` ausentes e UUID malformado; `INVALID_MESSAGE` para envelope inválido (pendência 9).
- **[Código] Sem `event_id`:** o comando é aceito e o alerta sai sem `event_id`, testado em `TestAlertaDeComando`.

### `ods.s3.alert_action.command` → `ods.s3.alert_action.reply`
- **[Oficial] Payload:** `alert_id` (UUID), `action` (`ACKNOWLEDGE`/`MUTE`/`CLOSE`), `actor` (`user:<id>`), `mute_until` (obrigatório em MUTE), `reason` (opcional).
- **[Oficial] Reply:** `{ "status": "OK", "alert_id": "…", "state": "…" }`. **Erros:** `ALERT_NOT_FOUND`, `INVALID_TRANSITION`.
- **[Código] Erros provisórios:** `INVALID_PAYLOAD` para `action` desconhecida, `actor` fora do formato, `mute_until` ausente e `alert_id` malformado.
- **[Código] Transições:** seguem a tabela `TRANSICOES` do VER-3, que a especificação não define (pendência 7).

### `ods.s3.alerts.query` → `ods.s3.alerts.reply`
- **[Oficial] Filtros:** `from_at`, `to_at`, `state`, `severity`, `limit` (no máximo 500), todos opcionais.
- **[Oficial] Reply:** `{ "status": "OK", "alerts": [ <payload de ods.s3.alert> ], "next_from_at": "…" }`. A especificação não lista códigos de erro.
- **[Código] Filtro e paginação:** o filtro usa `created_at` no intervalo `[from_at, to_at)`, e `next_from_at` só aparece quando há próxima página. A página termina antes do milissegundo do `next_from_at`, para não repetir alertas. O padrão de `limit` é 100. Os erros são provisórios (`INVALID_PAYLOAD`).
- **[Pendente] Lacunas:** a semântica de `from_at`/`to_at`, o desempate dentro do mesmo milissegundo e a falta de códigos de erro (pendências 15 e 16).

### Regras comuns aos comandos e à consulta
- **[Oficial] Reply e idempotência:** todo comando recebe reply e é idempotente pelo `message_id`. A reply leva `correlation_id` igual ao `message_id` do pedido e não tem campos de sinal (seção 2, regra 6; seção 3).
- **[Código] Reply mesmo com envelope inválido:** a resposta é `INVALID_MESSAGE`. Ficam sem reply só a mensagem de outro schema, a de outro `target` e a sem `message_id` UUID.
- **[Código] Comando repetido:** recebe a mesma reply. A consulta não é registrada, porque não altera estado.

## 3. Relatório: produtores permitidos

### O que a especificação define
- **[Oficial]** Na seção 9, as listas de **consumidores** são declaradas fechadas: "quem não aparece não consome".
- **[Oficial]** A mesma tabela também lista quem **envia** cada comando:
  - `alert.command`: A1, A2, A3, A5, A7;
  - `alert_action.command`: A3, A5, A7, isto é, **A1 e A2 não**;
  - `alerts.query`: A1, A2, A3, A5, A7;
  - `ods.s2.event`: só S2.
- **[Oficial]** A especificação **não define** o que fazer com um remetente fora da lista. Não há código de erro, nem regra de descarte, nem menção a autenticação de `producer`.
- **[Oficial]** B3 é responsável por "tópicos, entrega, retenção, **ACLs** e operação do broker" (manual, seção 3; frente B3).

### O que o código faz
- **[Código]** `ods.s2.event` só é aceito com `producer = "S2"`; qualquer outro é descartado com log (`ingestion.py`).
- **[Código]** Nos três comandos, o `producer` só é validado como texto não vazio. Conferido em 20/10/2026: `alert.command` vindo de `A8`, `S4` ou `XYZ` recebe `OK`, e `alert_action.command` vindo de `A2` também.
- **[Código]** Isso **diverge das listas da seção 9**, mas não há como corrigir sem inventar um código de erro.

### Validação contratual não é autenticação
| Camada | Pergunta | Quem resolve | Situação |
|---|---|---|---|
| Validação contratual | O `producer` declarado está na lista do contrato? | S3, com o código de erro definido por B1 | Pendência 19 |
| Autenticação | A mensagem veio mesmo de quem diz `producer`? | B3: credenciais por aplicação no broker | Pendência 19. O campo `producer` é só declarado e qualquer cliente pode escrever qualquer valor |
| Autorização de tópico | Esse cliente pode escrever em `ods.s3.alert_action.command`? | B3: ACL por tópico | Pendência 19 |
| Autorização de pessoa | O `actor` (`user:maria`) pode silenciar ou encerrar? | A5 e as demais aplicações, no backend | Pendência 8 |

### Opções para rejeitar um produtor inesperado [Recomendação]
| Opção | Como | Prós | Contras |
|---|---|---|---|
| A | Reply `ERROR` com um código novo, definido por B1 na tabela de erros do S3 | Explícito; quem enviou sabe por que falhou | Precisa de aprovação; é acréscimo de código, que sobe MINOR |
| B | Reply `ERROR` com o provisório `INVALID_MESSAGE` (o `producer` estaria "fora do contrato") | Não cria código novo | Mistura erro de formato com erro de permissão; continua provisório |
| C | Descartar sem reply e registrar em log | Simples | **Viola a seção 2, regra 6** (todo comando recebe reply) |
| D | ACL no broker: só as credenciais de A3, A5 e A7 escrevem em `alert_action.command` | É a única que autentica de verdade; a mensagem nem chega ao S3 | Depende de B3 e da configuração da build (P3) |

**Proposta:** a opção **D** como controle de segurança e a **A** como defesa no S3, para quando a ACL não estiver configurada. Até haver decisão, o S3 continua aceitando, e a divergência fica registrada aqui. Num ambiente de produção, cada build só tem a própria aplicação; na build do A5, por exemplo, só o A5 deveria enviar comandos ao S3. Restringir o S3 ao `producer` da própria build seria configuração da build, não regra do contrato.

## 4. Decisões externas ligadas a esta matriz

Os problemas completos, com equipe, pergunta e impacto, estão na seção "Pendências com outras equipes" do [README.md](README.md). A matriz depende principalmente das pendências 4, 5, 6, 7, 8, 9, 12, 13, 15, 16, 17, 18 e 19.

O roteiro de integração está em [CHECKLIST_INTEGRACAO_S3.md](CHECKLIST_INTEGRACAO_S3.md).
