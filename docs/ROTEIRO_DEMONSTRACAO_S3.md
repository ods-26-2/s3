# Roteiro de demonstração do S3 (Kafka local)

O que mostrar, o que digitar e onde tirar a captura. Tudo roda na sua
máquina: o Kafka no Docker, e o S3, o S2 simulado e o A5 simulado no
PowerShell. O broker da TRA-1 não é usado.

Captura de tela: **Win + Shift + S**. Vídeo: **Win + Alt + R**.

## Antes de começar

- Docker Desktop aberto, com **Engine running**.
- Três terminais na pasta do S3 (no VS Code: botão **+** do terminal ou
  **Dividir**). Em cada um, ative o `.venv`:

```powershell
cd "C:\Users\cgt44\OneDrive\Área de Trabalho\ODS geral\s3"
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.venv\Scripts\Activate.ps1
```

| Terminal | Papel |
|---|---|
| **T1** | o S3 |
| **T2** | a aplicação A5 (recebe os alertas) |
| **T3** | o S2 e o S4 (mandam eventos), os comandos do A5 e a consulta ao banco |

Nenhum outro S3 pode estar ligado (por exemplo, o `teste_kafka_local.py`).

## Parte 0 — Preparar

No **T3**:

```powershell
docker compose -f infra/kafka-local/docker-compose.yml up -d
Remove-Item s3-local.sqlite3* -ErrorAction SilentlyContinue
python tools/kit_aplicacoes.py preparar --zerar
```

No **T1**: `python tools/kit_aplicacoes.py s3`
No **T2**: `python tools/kit_aplicacoes.py aplicacao`

📸 **Captura 1:** Docker Desktop → **Containers**, com `s3-kafka-local` em verde.
Mostra: um Kafka de verdade rodando.

## Parte 1 — Receber do S2, criar o alerta e salvar no banco

No **T3**, com os seus dados:

```powershell
python tools/kit_aplicacoes.py evento --zona doca-3 --equipamento capacete --duracao 12
```

Guarde o `event_id` que aparece.

- **T1** mostra `entrada schema=ods.s2.event ... resultado=alerta_criado` e
  `saida schema=ods.s3.alert ... state=CREATED`.

```powershell
python tools/kit_aplicacoes.py banco
```

📸 **Captura 2:** T3 (evento + banco) e T1, lado a lado.
Mostra: o S3 recebeu o evento, aplicou o ROUTING (CONFIRMED → HIGH, SCREEN e PUSH,
título "EPI ausente: capacete") e gravou o alerta e o histórico no SQLite. Fila
de saída 0 = o Kafka confirmou a publicação.

**Decisões do S3 com eventos** (opcional, uma captura do T1 basta):

```powershell
python tools/kit_aplicacoes.py evento --status UNCERTAIN                 # T1: resultado=sem_alerta
python tools/kit_aplicacoes.py evento --event-id <event_id> --fase UPDATED  # T1: resultado=alerta_existente
python tools/kit_aplicacoes.py evento --event-id <event_id> --fase ENDED    # T1: alerta_existente, não fecha
python tools/kit_aplicacoes.py banco                                      # continua 1 alerta, CREATED
```

📸 **Captura 3:** T1 com as três linhas.
Mostra: UNCERTAIN não vira alerta (a rota pede CONFIRMED); o mesmo `event_id`
não cria um segundo alerta; o fim da violação não encerra o alerta (quem
encerra é uma pessoa, com CLOSE).

## Parte 2 — Passar o alerta para a aplicação

O **T2** já mostrou, na Parte 1, `[A5] NOVO ALERTA <alert_id>`, com título,
destinatários e canais. **Copie o `alert_id` completo** daqui: ele é usado nos comandos.

Agora o S4 manda o vídeo do evento. No **T3**:

```powershell
python tools/kit_aplicacoes.py clipe <event_id>
```

- **T1:** `resultado=vinculado`.
- **T2:** `[A5] ATUALIZAÇÃO`, mesmo `state`, com `clip_id`.

📸 **Captura 4:** T2.
Mostra: o S3 publica o alerta para a aplicação em `ods.s3.alert` e republica
quando o clipe do S4 chega (S3.7: alerta, evento e evidência associados).

## Parte 3 — Receber comandos, decidir e gravar

No **T3** (troque `<alert_id>` pelo copiado do T2):

```powershell
python tools/kit_aplicacoes.py acao <alert_id> ACKNOWLEDGE --motivo "a caminho"
python tools/kit_aplicacoes.py acao <alert_id> MUTE --actor user:ana
python tools/kit_aplicacoes.py acao <alert_id> MUTE --actor user:maria
python tools/kit_aplicacoes.py acao <alert_id> CLOSE --motivo "EPI colocado"
python tools/kit_aplicacoes.py acao <alert_id> ACKNOWLEDGE
```

| Comando | Resposta do S3 | Por quê |
|---|---|---|
| ACK da maria | `OK: ACKNOWLEDGED` | supervisora pode |
| MUTE da ana | `ERRO UNAUTHORIZED_ACTOR` | técnica de segurança só pode dar ACK (arquivo de permissões) |
| MUTE da maria | `OK: MUTED` (30 min) | permitido a partir de ACKNOWLEDGED |
| CLOSE da maria | `OK: CLOSED` | encerramento manual |
| ACK depois do CLOSE | `ERRO INVALID_TRANSITION` | CLOSED é final |

- **T2** mostra cada mudança e "o backend do A5 para o som e o push".

```powershell
python tools/kit_aplicacoes.py banco
python tools/kit_aplicacoes.py consulta
```

📸 **Captura 5:** T3 com as respostas dos cinco comandos.
📸 **Captura 6:** T3 com `banco` (histórico com ACK, MUTE e CLOSE, autores e motivos).
📸 **Captura 7:** T3 com `consulta` (o A5 pede o histórico pelo contrato `ods.s3.alerts.query`).
Mostra: S3.4 (comandos, autorização, ciclo de vida) e S3.3 (histórico gravado e
consultável). Os comandos negados não mudam nada no banco.

## Parte 4 — Robustez

**O S3 cai e volta sem perder nada.** No **T1**, **Ctrl+C**. No **T3**:

```powershell
python tools/kit_aplicacoes.py evento --equipamento oculos
```

No **T1**, de novo: `python tools/kit_aplicacoes.py s3`. Ele processa o evento que
chegou enquanto estava fora, e o **T2** recebe o alerta novo. Se o S3 anterior
não saiu pelo Ctrl+C (janela fechada, processo morto), o Kafka espera uns 45 s
antes de entregar as mensagens ao novo: é normal o alerta demorar até 1 minuto.

📸 **Captura 8:** T1 depois de voltar, com `alerta_criado`.
Mostra: S3.6 — nada se perde com o S3 fora do ar.

**As regras de consumo da aplicação.** No **T3**:

```powershell
python tools/kit_aplicacoes.py cenario D1 D4
```

**T2:** "mensagem repetida: descartada" e "sequence menor: ignorada (chegou fora de ordem)".

📸 **Captura 9:** T2.
Mostra: como a aplicação deve tratar repetição e ordem (o que o kit entrega ao A5).

## Parte 5 — Teste automático, com queda do broker

Pare o **T1** e o **T2** (Ctrl+C). No **T3**:

```powershell
python tools/teste_kafka_local.py --queda
```

📸 **Captura 10** (ou vídeo): todos os `ok` até "Tudo certo".
Mostra: fluxo completo, queda do broker e queda do S3, sem perder nem duplicar.

## Parte 6 — O que já está pronto

```powershell
$env:PYTHONPATH = "src"
python -m pytest tests/s3 -q
python -m pytest tests/s3/test_exemplo_s4.py -v
```

📸 **Captura 11:** `384 passed` (ou o número atual). Nenhum teste usa broker.
📸 **Captura 12:** os 5 testes do exemplo real do S4 (rejeitado pelos 4 motivos, aceito corrigido).
📸 **Captura 13:** GitHub, página de commits da branch (`github.com/ods-26-2/s3/commits/<branch>`).
📸 **Captura 14:** `docs/SITUACAO_SPRINT3_S3.pdf`, página 1 (situação de cada entrega).

## Ao terminar

```powershell
docker compose -f infra/kafka-local/docker-compose.yml stop
```

## O que esta demonstração não prova

O S2, o S4 e o A5 são simulados e o Kafka é local. Falta: o broker oficial
(TRA-1), o S2 e o S4 reais, e o backend do A5 tocando o som e mandando o push
de verdade (S3.5).
