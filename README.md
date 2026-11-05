# S3 — Alertas e Notificações | VER-3 | ODS 2026/2

[![CI](https://github.com/ods-26-2/s3/actions/workflows/ci.yml/badge.svg)](https://github.com/ods-26-2/s3/actions/workflows/ci.yml)

**Estado em 05/11/2026.** Referência: *Especificação de interfaces 1.0, arquitetura 6c*. Este documento distingue implementação local, teste automatizado e validação externa. **O S3 está implementado, rodou contra um Kafka local (Docker) e está pronto para o servidor: imagem Docker, script de instalação e verificação no broker testados. Ainda não rodou no broker oficial nem foi validado ponta a ponta com o S2 e o A5 reais: falta o servidor.**

Situação detalhada: [docs/SITUACAO_SPRINT3_S3.pdf](docs/SITUACAO_SPRINT3_S3.pdf) · Servidor: [docs/PASSO_A_PASSO_SERVIDOR_S3.pdf](docs/PASSO_A_PASSO_SERVIDOR_S3.pdf)

## 1. Visão geral

O S3 recebe ocorrências qualificadas de `ods.s2.event`, aplica regras de ROUTING versionadas para criar alertas e publica `ods.s3.alert`. Também permite criação direta por aplicações, comandos de reconhecimento/silenciamento/encerramento e consultas de histórico. Alertas e operações são armazenados em SQLite; publicações são registradas em uma *transactional outbox* para recuperação de falhas.

Fluxos principais:

| Origem → destino | Contrato ou artefato | Papel do S3 |
|---|---|---|
| S2 → S3 | `ods.s2.event` | Consome ocorrência e aplica regras; trata `STARTED`, `UPDATED`, `ENDED` e `CONFIRMED`/`UNCERTAIN` |
| A3/A5/A7 → S3 | `ROUTING` | Carrega regras de evento, status mínimo, severidade, destinatários, canais, textos e captura; **por arquivo, na partida** |
| S3 → aplicações | `ods.s3.alert` | Publica criação e atualizações; mantém `alert_id` na vida do alerta e usa novo `message_id` por publicação |
| A1/A2/A3/A5/A7 → S3 | `ods.s3.alert.command` / `.reply` | Criação direta de alertas de negócio e resposta |
| A3/A5/A7 → S3 | `ods.s3.alert_action.command` / `.reply` | `ACKNOWLEDGE`, `MUTE`, `CLOSE` e resposta |
| A1/A2/A3/A5/A7 → S3 | `ods.s3.alerts.query` / `.reply` | Histórico filtrado por período, estado e severidade, até 500 resultados por consulta |
| S4 → S3 (extensão aprovada em 21/10, a escrever na especificação) | `ods.s4.clip` | Correlaciona por `event_id` e associa `clip_id`; não transfere nem armazena MP4 |

A2 comunicou que **não pretende utilizar** o S3 nesta integração; isso não remove automaticamente sua compatibilidade contratual.

## 2. O que já está implementado

- **Contratos e validação:** envelopes 6c, validação de campos, enums, UUIDs, horários e produtores específicos; compatibilidade de saída e dos comandos, com exemplos em `tests/s3/dados/` e em `contratos/`.
- **Criação e ciclo de vida:** evento do S2, criação direta por comando, ACK, MUTE e CLOSE; armazenamento de responsável, motivo e horários; regras de estado em [docs/DECISOES_S3.md](docs/DECISOES_S3.md).
- **ROUTING:** leitura de artefato JSON local, com seleção por evento e status, severidade `LOW/MEDIUM/HIGH`, destinatários `user:/group:/role:`, canais `SOUND/SCREEN/PUSH`, mensagens e opção de captura.
- **Autorização:** quem pode reconhecer, silenciar e encerrar vem de um arquivo de permissões por build (`--permissoes`); negado = `UNAUTHORIZED_ACTOR`. Opcionalmente, `--produtores A3,A5,A7` faz o S3 aceitar comandos só dessas aplicações (o broker não tem autenticação); desligado por padrão.
- **Banco SQLite (`repository_sqlite.py`):** alertas, ações/histórico, respostas e IDs processados, clipes pendentes, sequências e outbox persistem após reinicialização. A implementação mantém também `repository.py` em memória para testes.
- **Confiabilidade:** escrita transacional com outbox, reenvio após falha/reinício e deduplicação; o comportamento é de **entrega pelo menos uma vez**, exigindo deduplicação também no consumidor. Encerra limpo com Ctrl+C e com SIGTERM (`docker stop`).
- **Histórico:** `ods.s3.alerts.query / .reply` com filtros e paginação. Não há contrato público separado para a trilha de auditoria; ela é armazenada internamente.
- **S4:** correlação assíncrona por `event_id`, incluindo clipes que chegam após o alerta. O alerta publica apenas `clip_id`, sem `media_uri`. O exemplo real do S4 é testado em `tests/s3/test_exemplo_s4.py`.
- **Kafka:** `kafka_adapter.py`, um tópico por schema (nomenclatura da especificação, **confirmada pela TRA-1 em 04/11**), grupo configurável, chave `alert_id`, commit manual depois de processar. Testado contra um Kafka local no Docker, com queda do broker e do S3 ([docs/TESTE_KAFKA_LOCAL.md](docs/TESTE_KAFKA_LOCAL.md)).
- **Servidor:** imagem Docker, compose com reinício automático e banco em volume, e `infra/servidor/instalar.sh`, que instala o Docker se faltar, cria os tópicos do S3 e liga o contêiner. Testado num Ubuntu 24.04 limpo, contra um Kafka sem tópicos.
- **Verificação no broker:** `tools/verificar_broker.py` prova o S3 num broker real com 4 comandos e 1 consulta, no papel do A5; fora de `localhost`, só roda com `--broker-oficial`.
- **Integração contínua:** a cada push, o GitHub roda os testes com cobertura e constrói a imagem ([.github/workflows/ci.yml](.github/workflows/ci.yml)).

## 3. Organização do código

| Local | Responsabilidade |
|---|---|
| `src/s3/service.py` | Regras de negócio, estados, associação de clipes e despacho da outbox |
| `src/s3/repository_sqlite.py` | Persistência SQLite e transações |
| `src/s3/repository.py` | Repositório em memória e regras comuns |
| `src/s3/history.py` | Registros de histórico/auditoria |
| `src/s3/contracts.py`, `envelope.py`, `output.py` | Contratos, envelopes e publicação serializada |
| `src/s3/commands.py`, `ingestion.py` | Comandos, consultas e ingestão de eventos/clipes |
| `src/s3/permissoes.py`, `rastreio.py` | Permissões de ação e uma linha de log por mensagem |
| `src/s3/routing.py` | Artefato ROUTING |
| `src/s3/kafka_adapter.py`, `subscription.py` | Adaptadores e assinaturas do transporte |
| `src/s3/__main__.py` | Ponto de entrada da execução com SQLite e Kafka |
| `tests/s3/` | Testes unitários, contratuais e de integração local com fakes |
| `contratos/` | Contratos e exemplos enviados às outras equipes |
| `infra/kafka-local/`, `infra/servidor/`, `Dockerfile` | Kafka local para testes; implantação no servidor |
| `tools/` | Teste no Kafka local, kit para as aplicações, verificação no broker e geradores dos PDFs |
| `docs/` | Decisões, guias e relatórios |

## 4. Como executar e testar

Na raiz do repositório, em PowerShell, com Python e dependências instaladas (`pip install -r requirements-dev.txt`):

```powershell
$env:PYTHONPATH = "src"
python -m pytest tests/s3 -q
```

**Resultado conferido em 05/11/2026:** `394 passed, 179 subtests passed` (0 falhas), com **98% do código coberto**. Nenhum teste da suíte acessa broker. O CI roda a mesma suíte a cada push. Os testes cobrem persistência SQLite e recuperação por reinicialização, mas **não** demonstram o broker oficial, a execução dos canais SOUND/SCREEN/PUSH nem a integração real com S2 e aplicações.

Fora da suíte, com Docker:

- `tools/teste_kafka_local.py` roda o S3 contra um Kafka local e simula S2, S4 e A5, inclusive com queda do broker e do S3 ([docs/TESTE_KAFKA_LOCAL.md](docs/TESTE_KAFKA_LOCAL.md)).
- `tools/kit_aplicacoes.py` faz o papel do S2, do S4 e do A5 (alertas, comandos, consulta) e mostra o banco ([docs/KIT_APLICACOES_KAFKA_LOCAL.md](docs/KIT_APLICACOES_KAFKA_LOCAL.md), [docs/ROTEIRO_DEMONSTRACAO_S3.md](docs/ROTEIRO_DEMONSTRACAO_S3.md)).
- Os dois só aceitam `localhost`.

**No servidor / na Jetson**, ligado ao broker do projeto (**Kafka da Jetson, `172.18.4.56:9092`, só na rede do DComp**, desde 06/11; antes, a nuvem de testes `34.66.235.91:9093`). A imagem funciona em ARM; sem internet na Jetson, veja "Sem internet" em [docs/IMPLANTACAO_SERVIDOR.md](docs/IMPLANTACAO_SERVIDOR.md):

```bash
git clone https://github.com/ods-26-2/s3.git && cd s3
bash infra/servidor/instalar.sh 172.18.4.56:9092
docker exec ods-s3 python tools/verificar_broker.py --broker-oficial
```

Plano B, num notebook com Windows na rede do DComp: `powershell -ExecutionPolicy Bypass -File infra\servidor\ligar_windows.ps1 172.18.4.56:9092`.

Passo a passo, problemas comuns e economia de créditos: [docs/PASSO_A_PASSO_SERVIDOR_S3.pdf](docs/PASSO_A_PASSO_SERVIDOR_S3.pdf) e [docs/IMPLANTACAO_SERVIDOR.md](docs/IMPLANTACAO_SERVIDOR.md).

Sem Docker, `python -m s3` aceita `--routing`, `--versao-routing`, `--permissoes`, `--produtores`, `--banco`, `--max-mensagens` e `--tempo-limite` (um dos dois últimos é obrigatório), e as variáveis `S3_KAFKA_BOOTSTRAP`, `S3_KAFKA_GROUP_ID` e `S3_PRODUTORES_COMANDOS`. Sem `--routing`, o S3 atende só comandos e consultas, sem assinar S2 e S4. Os arquivos de ROUTING e permissões em `contratos/` e `tests/s3/dados/` são **exemplos do A5**, não a política definitiva de cada aplicação.

O SQLite pressupõe **um processo por arquivo de banco**, e o broker, **um S3 só**: dois S3 ligados ao mesmo broker respondem aos mesmos comandos, cada um com o seu banco.

## 5. Decisões de comportamento vigentes na implementação

- `ENDED` do S2 **não fecha automaticamente** um alerta; o encerramento operacional é feito por comando `CLOSE`. `ENDED` sem alerta existente não cria alerta.
- `ACKNOWLEDGE` é permitido em `CREATED`; `MUTE` em `CREATED` ou `ACKNOWLEDGED`; `CLOSE` em estados não encerrados. Ao atingir `mute_until`, o estado não se altera automaticamente; a aplicação consumidora decide o efeito sobre a notificação.
- O S3 emite **metadados dos canais** (`SOUND`, `SCREEN`, `PUSH`); quem toca o som, mostra na tela e envia o push é o backend de cada aplicação.
- Para alertas criados diretamente por comando, o S3 permite ausência de `session_id` (aprovado em 21/10); falta constar na especificação e no validador de B1.
- O consumo de `ods.s4.clip` pelo S3 foi aprovado em 21/10; falta constar na matriz oficial de consumidores.
- O ROUTING é carregado no início da execução. Não existe atualização automática por `ods.artifact.published`.
- A paginação por `next_from_at` tem limitação conhecida quando mais de `limit` alertas possuem o mesmo timestamp em milissegundos; resolver integralmente pode exigir revisão compartilhada de contrato.

Detalhes e origens das decisões: [docs/DECISOES_S3.md](docs/DECISOES_S3.md).

## 6. Retorno das equipes (até 05/11/2026)

| Equipe | Retorno | Consequência |
|---|---|---|
| Aplicações | Contratos enviados (o que mandam e recebem do S3); nenhuma objeção | Falta o teste com os backends reais |
| A5 | Contratos OK | Regras do ciclo de vida confirmadas |
| A2 | Não utilizará S3 | Fora da integração ativa nesta sprint |
| A3 | Precisa do histórico de alertas | Usa `ods.s3.alerts.query` |
| S4 | Validou campos e correlação por `event_id`; clipe geralmente chega depois; `media_uri` é referência lógica `s4://` | Associação tardia por `clip_id`; o S4 ainda ajusta IDs, timestamps e envelope |
| TRA-1 (04/11) | Contratos aparentemente compatíveis; broker sem autenticação; tópicos seguem a especificação | Nenhuma mudança no código para os tópicos |
| TRA-1 (05/11) | Livres para criar os tópicos (o broker também cria sozinho); o S3 pode ficar ligado | O script de instalação cria os tópicos antes de ligar o S3 |
| S2, A7, B1 (validador), B2 | Sem confirmação conclusiva | Aguardar |

**Exemplo de S4 recebido em 29/10:** ainda diverge do envelope 6c (`captured_at`, `sequence`, formato UUID para `session_id` e `clip_id`). Não afrouxar o validador sem decisão formal.

## 7. Situação das entregas da Sprint 3

| Entrega | Estado atual | O que falta para aceite |
|---|---|---|
| **S3.1 — Contratos e ROUTING** | Implementado e testado localmente; TRA-1 achou os contratos compatíveis | Validador oficial de B1; ROUTING definitivo de cada aplicação |
| **S3.2 — Barramento B3** | Testado no Kafka local; broker liberado; servidor e verificação prontos | Servidor; rodar a verificação no broker oficial; S2 e A5 reais |
| **S3.3 — Banco e histórico** | SQLite e consulta testados; banco em volume no servidor | Teste no ambiente de integração |
| **S3.4 — Comandos e estados** | Implementado e testado, com autorização do actor | Teste com aplicações reais; ligar a lista de produtores se a TRA-1 concordar |
| **S3.5 — Canais** | O S3 publica os canais | O backend do A5 demonstrar som, tela e push |
| **S3.6 — Confiabilidade** | Testado no Kafka local com queda do broker e do S3 | Repetir no broker oficial |
| **S3.7 — Evidências** | Associação por `event_id`/`clip_id`, testada com o exemplo real do S4 | Saída do S4 corrigida; formalizar o S3 como consumidor |

## 8. Próximas ações

1. **Servidor:** quando chegar, rodar `instalar.sh` e a verificação no broker; guardar a captura como evidência do S3.2.
2. **S3.5:** o backend do A5 consumir `ods.s3.alert` e demonstrar som, tela e push, usando o kit.
3. **S2 real:** teste S2 → S3 → A5 quando o S2 publicar.
4. **TRA-1:** validador de B1, itens a escrever na especificação e acordo sobre a lista de produtores.

## 9. Referências internas

- [docs/DECISOES_S3.md](docs/DECISOES_S3.md) — decisões, ressalvas e confirmações externas.
- [docs/MATRIZ_CONTRATOS_S3.md](docs/MATRIZ_CONTRATOS_S3.md) — contratos, produtores/consumidores e dependências.
- [docs/CHECKLIST_INTEGRACAO_S3.md](docs/CHECKLIST_INTEGRACAO_S3.md) — roteiro de integração.
- [docs/REVISAO_ITEM17_S3.md](docs/REVISAO_ITEM17_S3.md) — regras de identidade, atualização e deduplicação dos alertas.
- [contratos/CONTRATOS_S3.md](contratos/CONTRATOS_S3.md) — página dos contratos do S3.

**Regra para reportar andamento:** teste local aprovado não equivale a aceite de integração com outro componente; confirmação de uma equipe não equivale a teste de transporte real.
