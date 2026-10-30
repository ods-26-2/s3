# S3 — Alertas e Notificações | VER-3 | ODS 2026/2

**Estado em 30/10/2026.** Referência: *Especificação de interfaces 1.0, arquitetura 6c*. Este documento distingue implementação local, teste automatizado e validação externa. **O S3 já rodou contra um Kafka local (Docker), mas ainda não foi integrado ao broker oficial nem validado ponta a ponta com o S2 e o A5 reais.**

## 1. Visão geral

O S3 recebe ocorrências qualificadas de `ods.s2.event`, aplica regras de ROUTING versionadas para criar alertas e publica `ods.s3.alert`. Também permite criação direta por aplicações, comandos de reconhecimento/silenciamento/encerramento e consultas de histórico. Alertas e operações são armazenados em SQLite; publicações são registradas em uma *transactional outbox* para recuperação de falhas.

Fluxos principais:

| Origem → destino | Contrato ou artefato | Papel do S3 |
|---|---|---|
| S2 → S3 | `ods.s2.event` | Consome ocorrência e aplica regras; trata `STARTED`, `UPDATED`, `ENDED` e `CONFIRMED`/`UNCERTAIN` |
| A3/A5/A7 → S3 | `ROUTING` | Carrega regras de evento, status mínimo, severidade, destinatários, canais, textos e captura; **atualmente por arquivo local** |
| S3 → aplicações | `ods.s3.alert` | Publica criação e atualizações; mantém `alert_id` na vida do alerta e usa novo `message_id` por publicação |
| A1/A2/A3/A5/A7 → S3 | `ods.s3.alert.command` / `.reply` | Criação direta de alertas de negócio e resposta |
| A3/A5/A7 → S3 | `ods.s3.alert_action.command` / `.reply` | `ACKNOWLEDGE`, `MUTE`, `CLOSE` e resposta |
| A1/A2/A3/A5/A7 → S3 | `ods.s3.alerts.query` / `.reply` | Histórico filtrado por período, estado e severidade, até 500 resultados por consulta |
| S4 → S3 (extensão em formalização) | `ods.s4.clip` | Correlaciona por `event_id` e associa `clip_id`; não transfere nem armazena MP4 |

A2 comunicou que **não pretende utilizar** o S3 nesta integração; isso não remove automaticamente sua compatibilidade contratual.

## 2. O que já está implementado

- **Contratos e validação:** envelopes 6c, validação de campos, enums, UUIDs, horários e produtores específicos onde implementado; compatibilidade de saída e dos comandos, com exemplos em `tests/s3/dados/`.
- **Criação e ciclo de vida:** evento do S2, criação direta por comando, ACK, MUTE e CLOSE; armazenamento de responsável, motivo e horários; regras de estado documentadas em `DECISOES_S3.md`.
- **ROUTING:** leitura de artefato JSON local, com seleção por evento e status, severidade `LOW/MEDIUM/HIGH`, destinatários `user:/group:/role:`, canais `SOUND/SCREEN/PUSH`, mensagens e opção de captura.
- **Banco SQLite (`repository_sqlite.py`):** alertas, ações/histórico, respostas e IDs processados, clipes pendentes, sequências e outbox persistem após reinicialização. A implementação mantém também `repository.py` em memória para testes.
- **Confiabilidade:** escrita transacional com outbox, reenvio após falha/reinício e deduplicação; o comportamento é de **entrega pelo menos uma vez**, exigindo deduplicação também no consumidor.
- **Histórico:** `ods.s3.alerts.query / .reply` com filtros e paginação. Não há contrato público separado para consulta de toda a trilha de auditoria; ela é armazenada internamente.
- **S4:** correlação assíncrona por `event_id`, incluindo clipes que chegam após o alerta. O alerta publica apenas `clip_id`, sem `media_uri`.
- **Kafka:** adaptadores `kafka_adapter.py`, tópico provisório por schema, grupo configurável, commits manuais e interfaces exercitadas por clientes falsos. Testado contra um Kafka local no Docker, com queda do broker e do S3 (`docs/TESTE_KAFKA_LOCAL.md`). **Não há prova de entrega/consumo no Kafka oficial.**

## 3. Organização do código

| Local | Responsabilidade |
|---|---|
| `src/s3/service.py` | Regras de negócio, estados, associação de clipes e despacho da outbox |
| `src/s3/repository_sqlite.py` | Persistência SQLite e transações |
| `src/s3/repository.py` | Repositório em memória e regras comuns |
| `src/s3/history.py` | Registros de histórico/auditoria |
| `src/s3/contracts.py`, `envelope.py`, `output.py` | Contratos, envelopes e publicação serializada |
| `src/s3/commands.py`, `ingestion.py` | Comandos, consultas e ingestão de eventos/clipes |
| `src/s3/routing.py` | Artefato ROUTING |
| `src/s3/kafka_adapter.py`, `subscription.py` | Adaptadores e assinaturas do transporte |
| `src/s3/__main__.py` | Ponto de entrada da execução com SQLite e Kafka |
| `tests/s3/` | Testes unitários, contratuais e de integração local com fakes |

## 4. Como executar e testar

Na raiz do repositório, em PowerShell, com Python e dependências instaladas:

```powershell
$env:PYTHONPATH = "src"
python -m pytest tests/s3 -q
```

**Resultado conferido em 30/10/2026:** `384 passed, 167 subtests passed` (0 falhas). Nenhum teste da suíte acessa broker. Os testes cobrem persistência SQLite e recuperação por reinicialização, mas **não** demonstram broker real, execução dos canais SOUND/SCREEN/PUSH nem integração real ponta a ponta com S2 e aplicações.

Fora da suíte, com Docker: `tools/teste_kafka_local.py` roda o S3 contra um Kafka local e simula S2, S4 e A5, inclusive com queda do broker e do S3 (`docs/TESTE_KAFKA_LOCAL.md`). `tools/kit_aplicacoes.py` deixa o backend de uma aplicação testar contra o S3 nesse Kafka (`docs/KIT_APLICACOES_KAFKA_LOCAL.md`). Os dois só aceitam `localhost`.

A execução operacional prevista pelo código requer um broker configurado e um limite de execução:

```powershell
$env:PYTHONPATH = "src"
$env:S3_KAFKA_BOOTSTRAP = "HOST:PORTA" # substituir pelo endereço aprovado por B3
python -m s3 --routing tests/s3/dados/routing_a5.json --banco s3.sqlite3 --tempo-limite 60
```

O caminho ROUTING acima é **exemplo de teste**, não uma política definitiva da A5. Sem `--routing`, o programa atende comandos e consultas, não assina eventos S2/S4. Também existem `--versao-routing`, `--max-mensagens` e variável `S3_KAFKA_GROUP_ID`. A execução real pressupõe a biblioteca `confluent-kafka`, acesso e configuração fornecidos pelo B3. Não há Kafka oficial disponível no momento; o comando acima é **instrução de preparação, não teste já realizado**. Evitar operar um broker compartilhado sem coordenação.

O SQLite padrão é `s3.sqlite3`. A implementação pressupõe **um processo por arquivo de banco**; não assumir concorrência segura de múltiplas instâncias sem projeto adicional.

## 5. Decisões de comportamento vigentes na implementação

- `ENDED` do S2 **não fecha automaticamente** um alerta; o encerramento operacional é feito por comando `CLOSE`. `ENDED` sem alerta existente não cria alerta.
- `ACKNOWLEDGE` é permitido em `CREATED`; `MUTE` em `CREATED` ou `ACKNOWLEDGED`; `CLOSE` em estados não encerrados. Ao atingir `mute_until`, o estado não se altera automaticamente; aplicação consumidora decide o efeito sobre a notificação.
- O S3 emite **metadados dos canais** (`SOUND`, `SCREEN`, `PUSH`), não executa, por si só, reprodução de som, exibição ou push. Responsabilidade efetiva permanece sujeita ao alinhamento com A3/A5/A7.
- Para alertas criados diretamente por comando, o S3 permite ausência de `session_id`; é uma **exceção proposta/registrada**, que precisa estar refletida nos contratos e validadores compartilhados.
- Consumo direto de `ods.s4.clip` pelo S3 é uma extensão registrada localmente; falta conferir sua incorporação à matriz oficial de consumidores.
- O ROUTING é carregado no início da execução. Não existe atualização automática comprovada por `ods.artifact.published`.
- A paginação por `next_from_at` tem limitação conhecida quando mais de `limit` alertas possuem o mesmo timestamp em milissegundos; resolver integralmente pode exigir revisão compartilhada de contrato.

Detalhes e origens das decisões: [DECISOES_S3.md](DECISOES_S3.md).

## 6. Retorno das equipes (informado até 30/10/2026)

| Equipe | Retorno | Consequência |
|---|---|---|
| A5 | Contratos OK | Confirmada revisão dos contratos; falta teste com backend real |
| A2 | Não utilizará S3 | Não priorizar integração ativa nesta sprint |
| A3 | Precisa do histórico de alertas | Confirmar se consulta atual basta ou se exige auditoria detalhada |
| S4 | Validou campos e correlação por `event_id`; clipe geralmente chega depois; `media_uri` é referência lógica `s4://` | Manter associação tardia por `clip_id`; S4 ainda ajusta formato de IDs, timestamps e endpoint de acesso |
| S2, A7, B1, B2, B3 | Sem confirmação conclusiva recebida | Aguardar validação de contratos, regras e infraestrutura |

**Exemplo de S4 recebido em 29/10:** o descritor de demonstração ainda diverge de exigências do envelope 6c (`captured_at`, `sequence`, formato UUID para `session_id` e `clip_id`). Também é necessário alinhar `correlation_id` nas capturas. Não afrouxar o validador sem decisão formal. O S4 já reconheceu algumas pendências em sua implementação.

## 7. Situação das entregas da Sprint 3

| Entrega | Estado atual | O que falta para aceite |
|---|---|---|
| **S3.1 — Contratos e ROUTING** | Implementado/testado localmente | B1/S2 confirmarem interfaces e aplicações fornecerem ROUTING definitivo; formalizar exceções |
| **S3.2 — Barramento B3** | Adaptador implementado, testes com fakes | Credenciais, tópicos e configuração reais; entrega/consumo por S2 e A5 no broker |
| **S3.3 — Banco e histórico** | SQLite e consulta implementados/testados localmente | Teste de persistência no ambiente de integração e validação do A3 |
| **S3.4 — Comandos e estados** | Implementado/testado localmente | Teste com aplicações reais; confirmar autorização, política de `ENDED` e MUTE |
| **S3.5 — Canais** | Roteamento de metadados implementado | Confirmar quem executa SOUND/SCREEN/PUSH e comprovar entrega ao destinatário |
| **S3.6 — Confiabilidade** | Outbox/deduplicação persistentes implementadas/testadas localmente | Exercitar falhas de broker/reinício em ambiente de integração; auditar casos-limite |
| **S3.7 — Evidências** | Associação local por `event_id`/`clip_id` | Validar com saída real corrigida de S4 e formalizar S3 como consumidor |

## 8. Próximas ações prioritárias

1. **Auditar confiabilidade local:** verificar atomicidade entre clipe pendente, alerta e outbox; simular falhas, reentregas e reinícios; complementar testes.
2. **Preparar teste integrado local:** fluxo S2 → S3 → A5 simulado usando contratos oficiais, comandos ACK/MUTE/CLOSE e consulta do histórico.
3. **Coletar decisões externas:** confirmar `ods.s2.event` real, ROUTING por aplicação, significado do histórico para A3, política de canais e formalização com B1/B2.
4. **Integrar com B3 quando disponível:** validar tópicos, ACLs, ordenação, commit, entrega e recuperação no Kafka oficial.
5. **Executar ponta a ponta real:** S2 → S3 → A5 e, conforme necessidade confirmada, A3/A7; registrar evidências de aceite.

## 9. Referências internas

- [MATRIZ_CONTRATOS_S3.md](MATRIZ_CONTRATOS_S3.md) — contratos, produtores/consumidores e dependências.
- [CHECKLIST_INTEGRACAO_S3.md](CHECKLIST_INTEGRACAO_S3.md) — roteiro de integração.
- [DECISOES_S3.md](DECISOES_S3.md) — decisões, ressalvas e confirmações externas.
- [REVISAO_ITEM17_S3.md](REVISAO_ITEM17_S3.md) — regras de identidade, atualização e deduplicação dos alertas.

**Regra para reportar andamento:** teste local aprovado não equivale a aceite de integração com outro componente; confirmação de uma equipe não equivale a teste de transporte real.
