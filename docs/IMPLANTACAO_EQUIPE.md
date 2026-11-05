# S3 — Alertas e Notificações: instruções para a equipe de implantação

Componente do squad VER-3. Contato: Carlos Gabriel Teixeira (VER-3).

O S3 é um serviço Python que só conversa com o Kafka: lê eventos e comandos,
cria alertas, grava num SQLite e publica alertas e respostas. Não abre porta,
não tem interface e não usa GPU.

## Resumo

| Item | Valor |
|---|---|
| Repositório | `https://github.com/ods-26-2/s3`, branch `main` |
| Imagem | `Dockerfile` na raiz (`python:3.12-slim` + `confluent-kafka`) |
| Arquiteturas | `linux/arm64` (Jetson) e `linux/amd64`, testadas |
| Variável obrigatória | `S3_KAFKA_BOOTSTRAP=172.18.4.56:9092` |
| Volume | `/dados` (SQLite: alertas, histórico, fila de saída) |
| Portas | nenhuma; só saída para o broker |
| Reinício | **sempre** (`restart: always` ou `unless-stopped`); ver "Reinício" |
| Instâncias | **uma só** por broker |
| Recursos | ~18 MB de memória e CPU quase nula parado (amd64); imagem de ~200 MB |
| Build | precisa de internet (imagem base e PyPI) |

## Construir e rodar

```bash
git clone https://github.com/ods-26-2/s3.git && cd s3
docker build -t ods-s3 .

# criar os 9 tópicos do S3 que faltarem (opcional, recomendado; ver "Tópicos")
docker run --rm --entrypoint python ods-s3 tools/criar_topicos.py 172.18.4.56:9092

docker run -d --name ods-s3 --restart unless-stopped \
  -e S3_KAFKA_BOOTSTRAP=172.18.4.56:9092 \
  -v ods-s3-dados:/dados \
  ods-s3
```

Ou com o compose do repositório (lê o broker de `infra/servidor/.env`):

```bash
echo "S3_KAFKA_BOOTSTRAP=172.18.4.56:9092" > infra/servidor/.env
docker compose -f infra/servidor/docker-compose.yml up -d --build
```

O `infra/servidor/instalar.sh <broker>` faz tudo isso numa máquina Ubuntu/Debian/Jetson,
inclusive instalar o Docker e o plugin compose se faltarem.

## Variáveis de ambiente

| Variável | Obrigatória | Padrão | O que é |
|---|---|---|---|
| `S3_KAFKA_BOOTSTRAP` | sim | — | `host:porta` do broker. Sem ela, o S3 não liga |
| `S3_KAFKA_GROUP_ID` | não | `S3-alertas` | grupo de consumidores |
| `S3_PRODUTORES_COMANDOS` | não | vazio | se definido (ex.: `A3,A5,A7`), o S3 só atende comandos desses produtores; os outros recebem `INVALID_MESSAGE`. Deixar vazio até a TRA-1 decidir |

## Reinício

O processo **sai sozinho a cada 24 h, de propósito** (`--tempo-limite 86400` no
`CMD`), com o consumidor e o banco fechados, para reabrir limpo. Por isso a política
de reinício precisa religá-lo também após saída normal (código 0): `always` ou
`unless-stopped` funcionam; **`on-failure` não** (o S3 ficaria parado depois de 24 h).

Parar com `docker stop` (SIGTERM) leva ~1 s e não perde nada: o que não foi publicado
fica na fila de saída e sai na partida seguinte.

## Volume `/dados`

O SQLite fica em `/dados/s3.sqlite3`. Sem volume, alertas e histórico somem quando o
contêiner é recriado. O processo roda como o usuário `s3` (uid 1000): um volume nomeado
funciona direto; uma pasta do host montada em `/dados` precisa ter escrita para o uid 1000.

## Tópicos

O S3 lê `ods.s2.event`, `ods.s4.clip`, `ods.s3.alert.command`,
`ods.s3.alert_action.command` e `ods.s3.alerts.query`, e publica `ods.s3.alert`,
`ods.s3.alert.reply`, `ods.s3.alert_action.reply` e `ods.s3.alerts.reply` (nomes da
Especificação de interfaces 1.0, confirmados pela TRA-1).

O broker cria tópicos na primeira mensagem, mas um tópico criado **depois** que o S3
já está escutando pode levar até 5 min para ser percebido. Por isso o
`tools/criar_topicos.py` (dentro da imagem) cria os que faltam antes de ligar; tópico
que já existe não é tocado, e partições e réplicas ficam no padrão do broker.

## Configuração de regras e permissões

A imagem vem com a configuração de exemplo do A5:

- `/config/routing.json`: quais eventos viram alerta (ROUTING);
- `/config/permissoes.json`: quem pode reconhecer, silenciar e encerrar.

Para outra configuração, monte arquivos por cima desses dois caminhos (somente
leitura). Os argumentos do processo estão no `CMD` do `Dockerfile`.

## Conferir que está funcionando

```bash
docker logs ods-s3          # uma linha por mensagem que entra ou sai
docker exec ods-s3 python tools/verificar_broker.py --broker-oficial
```

A verificação faz o papel da aplicação A5: confere os 9 tópicos, cria um alerta de
teste por comando ("[TESTE S3]"), espera o alerta publicado, dá ACKNOWLEDGE e CLOSE (o
alerta de teste não fica aberto nas aplicações) e consulta o histórico. São 4 comandos e
1 consulta; termina em até 60 s com **"Tudo certo"** ou com o motivo da falha.

Na partida, o log mostra avisos esperados enquanto não houver configuração definitiva
(por exemplo, `sem --produtores: qualquer produtor pode mandar comandos`).

## Cuidados

- **Uma instância só por broker.** Dois S3 no mesmo broker respondem aos mesmos
  comandos, cada um com o seu banco, e as aplicações recebem respostas contraditórias.
- O banco pressupõe **um processo por arquivo**: não compartilhe o volume entre contêineres.
- Mais detalhes (systemd sem Docker, imagem ARM sem internet na máquina, problemas comuns):
  [IMPLANTACAO_SERVIDOR.md](IMPLANTACAO_SERVIDOR.md) e
  [PASSO_A_PASSO_SERVIDOR_S3.pdf](PASSO_A_PASSO_SERVIDOR_S3.pdf).
