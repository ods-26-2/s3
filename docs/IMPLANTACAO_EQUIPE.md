# S3 — Alertas e Notificações: instruções para a equipe de implantação

Componente do squad VER-3. Contato: Carlos Gabriel Teixeira (VER-3).

O S3 é um serviço Python que só conversa com o Kafka: lê eventos e comandos,
cria alertas, grava num SQLite e publica alertas e respostas. Não abre porta,
não tem interface e não usa GPU.

## Resumo

| Item | Valor |
|---|---|
| Imagem pronta | `ghcr.io/ods-26-2/s3:latest` (`linux/arm64` e `linux/amd64`), publicada pelo CI a cada push na `main` |
| Ou construir | `Dockerfile` na raiz do repositório `https://github.com/ods-26-2/s3`, branch `main` |
| Broker | na Jetson, `kafka:9092`: o nome que o Kafka anuncia desde 11/11/2026 |
| Rede | **na Jetson, a rede Docker do Kafka (obrigatório):** só nela o nome `kafka` funciona; ver "Rede na Jetson" |
| Tópicos | o S3 cria os que faltarem ao ligar |
| Volume | `/dados` (SQLite: alertas, histórico, fila de saída) |
| Portas | nenhuma; só saída para o broker |
| Reinício | `always` ou `unless-stopped` (só para quedas: o S3 roda continuamente) |
| Instâncias | **uma só** por broker |
| Recursos | ~18 MB de memória e CPU quase nula parado; imagem de ~200 MB |

## Rodar na Jetson

Troque `kafka_default` pelo nome da rede Docker do Kafka (`docker network ls`).

```bash
docker run -d --name ods-s3 --restart unless-stopped --network kafka_default   -e S3_KAFKA_BOOTSTRAP=kafka:9092 -v ods-s3-dados:/dados ghcr.io/ods-26-2/s3:latest
```

Ou com o compose da Jetson, a partir do repositório (`https://github.com/ods-26-2/s3`, branch `main`):

```bash
git clone https://github.com/ods-26-2/s3.git && cd s3
echo "KAFKA_REDE=kafka_default" > infra/jetson/.env      # só se a rede tiver outro nome
docker compose -f infra/jetson/docker-compose.yml up -d --build
```

**Antes de subir:** se o S3 estiver rodando pelo notebook de um integrante do VER-3 (ponte), ele
precisa ser desligado antes (`ligar_ponte_jetson.ps1 -Parar`). Dois S3 no mesmo Kafka respondem aos
mesmos comandos.

## Rede na Jetson

Desde 11/11/2026, o Kafka da Jetson anuncia `kafka:9092`: o nome do contêiner do Kafka na rede Docker
dele. Todo cliente, depois da primeira conexão, é mandado para `kafka:9092`. Só quem está na **mesma
rede Docker** resolve esse nome; um contêiner fora dela lê a lista de tópicos, mas criar tópico,
publicar e ler falham (`Failed while waiting for controller`). Por isso o S3 entra na rede do Kafka.

Histórico: até 06/11 o Kafka anunciava `localhost:9092`, e o S3 rodaria com `--network host`; a porta
9093 é a do controlador do Kafka, não de clientes. Fora da Jetson, o VER-3 usa uma ponte
(`infra/servidor/ligar_ponte_jetson.ps1`), testada em 11/11/2026 com "Tudo certo".

## Variáveis de ambiente (todas opcionais)

| Variável | Padrão | O que é |
|---|---|---|
| `S3_KAFKA_BOOTSTRAP` | `172.18.4.56:9092` na imagem; `kafka:9092` no compose da Jetson | `host:porta` do broker; na Jetson, `kafka:9092` |
| `S3_KAFKA_GROUP_ID` | `S3-alertas` | grupo de consumidores |
| `S3_PRODUTORES_COMANDOS` | vazio | se definido (ex.: `A3,A5,A7`), o S3 só atende comandos desses produtores; os outros recebem `INVALID_MESSAGE`. Deixar vazio até a TRA-1 decidir |

## Na partida

O log mostra, nesta ordem: os avisos de configuração (por exemplo,
`sem --produtores: qualquer produtor pode mandar comandos`), a conferência dos
tópicos (`tópicos do S3: criados ...` ou `todos já existiam`) e, depois, uma linha
por mensagem que entra ou sai. Se o broker não responder ao conferir os tópicos, o
S3 avisa e segue ligando.

Os tópicos são os da Especificação de interfaces 1.0 (confirmados pela TRA-1): o S3
lê `ods.s2.event`, `ods.s4.clip`, `ods.s3.alert.command`,
`ods.s3.alert_action.command` e `ods.s3.alerts.query`, e publica `ods.s3.alert`,
`ods.s3.alert.reply`, `ods.s3.alert_action.reply` e `ods.s3.alerts.reply`.

## Volume `/dados`

O SQLite fica em `/dados/s3.sqlite3`. Sem volume, alertas e histórico somem quando o
contêiner é recriado. O processo roda como o usuário `s3` (uid 1000): um volume nomeado
funciona direto; uma pasta do host montada em `/dados` precisa ter escrita para o uid 1000.

Parar com `docker stop` (SIGTERM) leva ~1 s e não perde nada: o que não foi publicado
fica na fila de saída e sai na partida seguinte.

## Configuração de regras e permissões

A imagem vem com a configuração de exemplo do A5:

- `/config/routing.json`: quais eventos viram alerta (ROUTING);
- `/config/permissoes.json`: quem pode reconhecer, silenciar e encerrar.

Para outra configuração, monte arquivos por cima desses dois caminhos (somente
leitura). Os argumentos do processo estão no `CMD` do `Dockerfile`.

## Conferir que está funcionando

```bash
docker logs ods-s3
docker exec ods-s3 python tools/verificar_broker.py --broker-oficial
```

A verificação faz o papel da aplicação A5: confere os 9 tópicos, cria um alerta de
teste por comando ("[TESTE S3]"), espera o alerta publicado, dá ACKNOWLEDGE e CLOSE (o
alerta de teste não fica aberto nas aplicações) e consulta o histórico. São 4 comandos e
1 consulta; termina em até 60 s com **"Tudo certo"** ou com o motivo da falha.

## Cuidados

- **Uma instância só por broker.** Dois S3 no mesmo broker respondem aos mesmos
  comandos, cada um com o seu banco, e as aplicações recebem respostas contraditórias.
- O banco pressupõe **um processo por arquivo**: não compartilhe o volume entre contêineres.
- **Rede Docker x rede do DComp.** O Kafka da Jetson está em `172.18.4.56`, e o Docker costuma
  criar redes de contêiner em `172.17–172.31.x.x`. Se existir na máquina uma rede Docker em
  `172.18.0.0/16`, o tráfego dos contêineres para o broker é desviado para ela e o S3 não conecta
  (`Failed to get metadata: Broker transport failure`), mesmo com a máquina alcançando o broker.
  Aconteceu num notebook em 06/11/2026. Conferir com
  `docker network inspect $(docker network ls -q) --format '{{.Name}} {{range .IPAM.Config}}{{.Subnet}}{{end}}'`;
  o compose do S3 já fixa a sua rede em `10.250.2.0/24`.
- Mais detalhes (systemd sem Docker, imagem ARM sem internet na máquina, problemas comuns):
  [IMPLANTACAO_SERVIDOR.md](IMPLANTACAO_SERVIDOR.md) e
  [PASSO_A_PASSO_SERVIDOR_S3.pdf](PASSO_A_PASSO_SERVIDOR_S3.pdf).
