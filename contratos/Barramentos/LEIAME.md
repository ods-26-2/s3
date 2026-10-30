# Exemplos de mensagens do S3, pelo nome do schema

Exemplos para a TRA-1 (B1 e B3) e a TRA-2 (B2). Os nomes dos arquivos são os da Especificação de interfaces 1.0 (arquitetura 6c). Cada arquivo é uma mensagem completa, do jeito que trafega no barramento, e passa no verificador de contrato do S3. A descrição fica aqui, e não dentro do JSON, porque o envelope só admite os 13 campos da seção 3.

| Arquivo | O que é | S3 |
|---|---|---|
| `Evento recebido do S2.json` | `ods.s2.event`: evento qualificado do S2 | recebe |
| `Evidência do S4.json` | `ods.s4.clip`: descritor do clipe do S4 | recebe |
| `ods.s3.alert.command.json` | Solicitação para criar alerta | recebe |
| `ods.s3.alert.reply.json` | Resposta à criação | envia |
| `ods.s3.alert_action.command.json` | Comando para agir sobre um alerta | recebe |
| `ods.s3.alert_action.reply.json` | Resposta à ação | envia |
| `ods.s3.alerts.query.json` | Solicitação do histórico | recebe |
| `ods.s3.alerts.reply.json` | Resposta do histórico | envia |
| `ods.s3.alert.json` | Alerta enviado às aplicações | envia |
| `ROUTING.json` | Regras de criação e encaminhamento (artefato, não é mensagem) | recebe |

Explicação de cada contrato: [CONTRATOS_S3.md](../CONTRATOS_S3.md). O documento em PDF para esta equipe é [CONTRATOS_S3_PARA_BARRAMENTO.pdf](CONTRATOS_S3_PARA_BARRAMENTO.pdf).
