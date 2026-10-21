# Revisão do item 17 da Especificação de interfaces 1.0 — S3 (VER-3)

**Para:** TRA-1 (B1 e B3) e TRA-2 (B2). **Cópia:** VER-4 (revisor do item 17), VER-6 (S2) e MOD-2 (S4).
**De:** VER-3, componente S3 — Alertas e Notificações.

O item 17 da seção 10 ("S3 cria alertas pelo artefato ROUTING e por comando; `event_id` opcional; ações e histórico por mensagem") é implementável, com as ressalvas abaixo. Comparamos o S3 campo a campo com a especificação. O código segue os contratos, menos no ponto 1, que não tem como ser resolvido sem decisão de vocês.

Cada ponto traz a referência, a pergunta e uma proposta. Nenhuma das propostas foi implementada como regra de contrato.

---

## 1. `session_id` em alerta criado por comando — **bloqueia A1, A2 e A7**
**Para:** TRA-2 (B2) e TRA-1 (B1)

- **Referência:** a seção 3 diz que o perfil EV-SERVIÇO exige `captured_at`, `session_id` e `sequence`, e que o perfil CMD vem "sem campos de sinal". A seção 7 diz que `ods.s3.alert` é EV-SERVIÇO e é criado também a partir de `ods.s3.alert.command`, que é CMD.
- **Problema:** o comando não pode trazer sessão, o S3 não consome `ods.session.started`, e um alerta de negócio ("acesso negado", "chamar garçom") nem nasce de uma sessão de câmera. O alerta sai sem `session_id` e o validador de B1 o rejeitaria.
- **Pergunta:** que `session_id` (e que `captured_at`) um alerta criado por comando deve levar?
- **Alternativas:**
  - **(A)** O S3 passa a consumir `ods.session.started` e usa a sessão atual da build. Resolve também o ponto 5.
  - **(B)** O comando ganha um `session_id` opcional, preenchido pela aplicação. É acréscimo de campo, sobe só o MINOR.
  - **(C)** O alerta de comando fica dispensado de `session_id` no perfil EV-SERVIÇO.
- **Proposta do VER-3:** (C), restrita:
  1. `session_id` opcional **só** em `ods.s3.alert` criado por `ods.s3.alert.command`; o alerta vindo de `ods.s2.event` continua exigindo o campo;
  2. `sequence` continua obrigatório e, sem sessão, conta só pelo produtor;
  3. `correlation_id` = `message_id` do comando criador, o que mantém a origem rastreável;
  4. `captured_at` = instante de criação do alerta, porque não há sinal capturado.

  Motivo: um alerta de negócio não nasce de nenhuma sessão de câmera. Na opção (A), o S3 teria que escolher uma "sessão atual" entre as várias que podem existir numa build (P4, P5, várias câmeras), e o alerta citaria uma sessão sem relação com ele.
- **Situação:** o S3 já segue essa regra desde 21/10/2026. Falta B1 aceitá-la no validador e B2 confirmar. A3, A5 e A7 precisam aceitar `ods.s3.alert` sem `session_id`.

## 2. "`alert_id` é chave de deduplicação"
**Para:** TRA-1 (B1)

- **Referência:** seção 2, regra 7.
- **Problema:** o S3 republica o alerta a cada mudança de estado, com o mesmo `alert_id` e `message_id` novo. Um consumidor que descarte pelo `alert_id` perde ACK, MUTE e CLOSE. O mesmo acontece com `event_id` em STARTED, UPDATED e ENDED.
- **Pergunta:** a regra quer dizer "descarte pelo `message_id` e atualize o registro pelo `alert_id`/`event_id`"?
- **Proposta:** escrever assim na regra 7. Não muda nenhum campo.

## 3. Ordem das mensagens do mesmo alerta
**Para:** TRA-1 (B3)

- **Referência:** seção 3 (`sequence` por `producer` + `session_id`) e frente B3 ("entrega, confirmação, ordenação").
- **Problema:** não há garantia de ordem entre as versões de um alerta. O `sequence` não ajuda em alerta sem `session_id` (ponto 1) e volta a 0 se o S3 reiniciar, enquanto não houver persistência.
- **Pergunta:** o B3 adota a chave de partição por entidade, isto é, `alert_id` em `ods.s3.alert` e `event_id` em `ods.s2.event`?
- **Situação:** o S3 já publica `ods.s3.alert` com chave `alert_id`. É só transporte e não altera a mensagem.

## 4. Remetente fora da lista do contrato
**Para:** TRA-1 (B1 e B3)

- **Referência:** a seção 9 lista quem envia cada comando, e `alert_action.command` vem só de A3, A5 e A7. A seção 2, regra 6, diz que todo comando recebe reply.
- **Problema:** a especificação não diz o que fazer com outro remetente. O campo `producer` é declarado pela mensagem e não autentica ninguém. Hoje o S3 aceita qualquer `producer` nos comandos.
- **Pergunta:** existe um código de erro para "remetente não previsto"? E o B3 vai aplicar ACL por tópico, com credencial por aplicação?
- **Proposta:** ACL no broker como controle principal, e reply `ERROR` com um código definido por B1 como defesa no S3. Descartar sem reply violaria a regra 6.

## 5. `ods.session.started` e estado da camada 3
**Para:** TRA-2 (B2)

- **Referência:** a seção 7 diz que a camada 3 "reinicia esse estado a cada `ods.session.started`". O contrato fala em "todo componente com estado". Mas a lista da seção 9 não inclui o S3, e o glossário diz "todo componente com estado zera".
- **Pergunta:** o S3 deve consumir `ods.session.started`? Se consumir, alertas abertos e o histórico **não** podem ser zerados num SEEK de replay, porque são auditoria. A regra vale para o S3?

## 6. `capture: true` e o `clip_id`
**Para:** TRA-1 e MOD-2 (S4)

- **Referência:** a seção 8 diz que, no ROUTING, `capture` serve para "vincular o `clip_id` que S4 gerou para o `event_id`". A seção 9 diz que o S3 consome só S2, e `ods.s4.clip` não lista o S3.
- **Pergunta:** o S3 deve consumir `ods.s4.clip`, ou a aplicação junta `alert.event_id` com `clip.event_id`?
- **Proposta:** a aplicação faz a junção (A5 já consome `ods.s4.clip`) e `capture` sai do ROUTING. A alternativa é incluir o S3 como consumidor de `ods.s4.clip`.

## 7. ROUTING do A7
**Para:** VER-4 e TRA-1

- **Referência:** a seção 8 diz "ROUTING (dono: A3, A5, A7)", e as atividades do A7 dizem "notificações por `ods.s3.alert.command` com o artefato ROUTING".
- **Problema:** o ROUTING é aplicado a `ods.s2.event`, e a build do A7 não tem o S2, que consome I2, I9 e S1.
- **Pergunta:** o A7 precisa mesmo de um ROUTING? Os alertas do A7 vêm todos por comando?

## 8. Pontos menores
**Para:** TRA-1 (B1)

| Ponto | Referência | Pergunta ou proposta |
|---|---|---|
| Códigos de erro do S3 | seção 7 | `alert.command` não tem código para `severity` inválida nem para campo ausente, e `alerts.query` não tem nenhum. O S3 usa provisoriamente `INVALID_PAYLOAD` (payload) e `INVALID_MESSAGE` (envelope). Podem entrar na tabela do S3? |
| `{duration_s}` no ROUTING | seção 8 | O modelo usa o campo, mas `ods.s2.event` só tem `duration_ms`. O S3 deriva `duration_ms // 1000`. Documentar como campo derivado? |
| Exemplos | seções 7 e 8 | O exemplo de `ods.s3.alert` ("no galpão de solda") não sai do modelo do exemplo de ROUTING ("em {zone_id}" dá "em galpao-solda") |
| Paginação | seção 7 | `next_from_at` é inclusivo? Como desempatar alertas criados no mesmo milissegundo? O S3 termina a página antes desse milissegundo |
| `correlation_id` em republicação | seções 3 e 7 | A seção 3 fala em "mensagem que causou" (seria o comando de ação); a seção 7 fixa o comando criador. O S3 segue a seção 7 |
| Tópicos e replies | frente B3 | "Um tópico por schema": o tópico tem o nome do schema (`ods.s3.alert`)? O Guia do Kafka usa outra nomenclatura (`ods.alerta.emitido`). As replies vão para `ods.s3.<x>.reply`, e quem pediu filtra pelo `correlation_id`? |
| Recarga do ROUTING | seção 8 | Quem consome recarrega o artefato ao receber `ods.artifact.published`, mas não há contrato para buscar o conteúdo pela `uri` (`registry://…`) |

---

Detalhes de cada ponto, com o comportamento atual e os testes: `MATRIZ_CONTRATOS_S3.md` e `CHECKLIST_INTEGRACAO_S3.md`, na pasta `docs/` do repositório do S3.
