# Implantação do S3 num servidor

Como deixar o S3 rodando fora da máquina de desenvolvimento, ligado ao broker
do projeto. **Desde 06/11/2026 é o Kafka da Jetson: `172.18.4.56:9092`**,
acessível só dentro da rede do DComp (e-mail do professor). Antes era a nuvem de
testes do Guia do Kafka da TRA-1 (`172.18.4.56:9092`, sem autenticação).

## Na Jetson

| Ponto | O que fazer |
|---|---|
| Arquitetura ARM (`aarch64`) | Nada: a imagem parte do `python:3.12-slim`, que existe para ARM, e a `confluent-kafka` tem versão para ARM. O script constrói a imagem na própria Jetson |
| Python da Jetson (JetPack 5 vem com 3.8; o S3 precisa de 3.10+) | Usar o Docker (o script), não o Python da Jetson |
| Docker sem o `docker compose` | O script tenta instalar o plugin (`docker-compose-v2` ou `docker-compose-plugin`) e para com instrução se não conseguir |
| Rede só do DComp | Instalar e testar de dentro da rede do DComp (na Jetson ou numa máquina de lá) |
| Sem internet na Jetson | Construir a imagem ARM fora e carregar: seção "Sem internet" abaixo |


## O que o S3 precisa

| Item | Valor |
|---|---|
| Sistema | Linux (ou qualquer um com Docker). Testado em Windows 11 e na imagem Docker (Linux), contra o Kafka local, em 04/11/2026 |
| Python | 3.10 ou mais novo, se rodar sem Docker |
| Dependência | só `confluent-kafka` (`requirements.txt`) |
| Rede | saída para `172.18.4.56:9092`. O S3 não abre porta: não precisa de entrada |
| Disco | uma pasta com escrita para o SQLite (alertas, histórico, fila de saída); poucos MB |
| Memória e CPU | medido no contêiner: 18 MB e 0,04% de CPU parado (o laço espera 1 s por mensagem). Imagem: 200 MB |
| Parar | `docker stop` / `systemctl stop` (SIGTERM): sai em ~1 s, fechando o consumidor e o banco; ao religar, volta a processar em segundos |
| Configuração | `S3_KAFKA_BOOTSTRAP` (obrigatória), `S3_KAFKA_GROUP_ID` (padrão `S3-alertas`), o arquivo de ROUTING e o de permissões |

**Uma instância só.** Com o S3 no servidor, não rode outro S3 contra o mesmo
broker (nem na sua máquina): dois S3 respondem aos mesmos comandos, cada um
com o seu banco, e a aplicação recebe respostas contraditórias.

## Sem internet

Construir a imagem exige baixar o Python e a `confluent-kafka`. Se a Jetson não
alcançar a internet, construa a imagem ARM num PC com Docker e leve o arquivo:

```bash
# no PC (Docker Desktop), na pasta do s3:
docker buildx build --platform linux/arm64 --output "type=docker,name=ods-s3:latest,dest=ods-s3-arm64.tar" .
# copie ods-s3-arm64.tar e a pasta do s3 para a Jetson; lá:
docker load -i ods-s3-arm64.tar
bash infra/servidor/instalar.sh 172.18.4.56:9092 --sem-construir
```

O arquivo sai com uns 47 MB e o nome certo (`ods-s3:latest`, `linux/arm64`), sem mexer
na imagem do PC. Testado em 05/11/2026: a imagem arm64, rodada por emulação, passou na
verificação inteira.

## Plano B — no notebook com Windows (na rede do DComp)

Se não der para instalar na Jetson, o S3 roda no notebook, com o Docker Desktop aberto,
conectado à rede do DComp. Ele fica no ar só enquanto o notebook estiver ligado e lá.

```powershell
powershell -ExecutionPolicy Bypass -File infra\servidor\ligar_windows.ps1 172.18.4.56:9092
docker exec ods-s3 python tools/verificar_broker.py --broker-oficial
```

O `ligar_windows.ps1` faz o mesmo que o `instalar.sh`, sem instalar nada: confere o Docker
Desktop (acha o `docker` mesmo fora do PATH), grava o `.env`, constrói a imagem, cria os
tópicos e liga o `ods-s3`. Testado em 05/11/2026 contra um Kafka sem tópicos: criou os 9,
ligou o S3 e a verificação passou. O `instalar.sh` recusa rodar no Windows.

## Jeito mais rápido — script de instalação (Ubuntu/Debian/Jetson)

```bash
git clone https://github.com/ods-26-2/s3.git && cd s3
bash infra/servidor/instalar.sh 172.18.4.56:9092
```

O `infra/servidor/instalar.sh` instala o Docker se faltar, guarda o broker em
`infra/servidor/.env` (fora do git), constrói e liga o `ods-s3`, confere se os
9 tópicos do S3 existem no broker (só lê a lista) e mostra o log. Testado em
04/11/2026 num Ubuntu 24.04 limpo, sem Docker, contra um Kafka de teste: o S3
instalado processou um evento e religou depois de `down`/`up` lendo o `.env`.
Para atualizar: `bash infra/servidor/instalar.sh 172.18.4.56:9092 --atualizar`.

**Aceitar comandos só de A3, A5 e A7** (desligado por padrão; ligar quando a TRA-1
concordar): acrescente `S3_PRODUTORES_COMANDOS=A3,A5,A7` ao `infra/servidor/.env` e
rode `docker compose -f infra/servidor/docker-compose.yml up -d`. Comandos de outro
`producer` passam a receber ERROR `INVALID_MESSAGE`. O log da partida mostra
"comandos aceitos só de: A3, A5, A7".

**Verificação mínima no broker** (uma vez, na janela combinada com a TRA-1):

```bash
docker exec ods-s3 python tools/verificar_broker.py --broker-oficial
```

Faz o papel do A5: confere os 9 tópicos, cria um alerta de teste por comando
("[TESTE S3]"), espera o `ods.s3.alert`, dá ACKNOWLEDGE e CLOSE como
`user:maria` (o alerta de teste não fica aberto nas aplicações) e consulta o
histórico. São 4 comandos e 1 consulta; os consumidores começam do fim dos
tópicos e tudo termina em 60 s. Não publica em `ods.s2.event`. Testado contra o
Kafka local em 04/11/2026: passou em 4 s. Se o S3 estiver desligado, falha com
"não chegou a resposta", e o comando de teste fica no tópico: o S3 cria esse
alerta quando ligar (encerre-o com CLOSE).

## Opção A — Docker, à mão

No servidor, com o repositório clonado (`git clone https://github.com/ods-26-2/s3.git`):

```bash
export S3_KAFKA_BOOTSTRAP=172.18.4.56:9092
docker compose -f infra/servidor/docker-compose.yml up -d --build
docker compose -f infra/servidor/docker-compose.yml logs -f s3
```

PowerShell: `$env:S3_KAFKA_BOOTSTRAP = "172.18.4.56:9092"` no lugar do `export`.

- O contêiner se chama `ods-s3`, reinicia sozinho se cair (`restart: unless-stopped`)
  e sai a cada 24 h para reabrir limpo (`--tempo-limite 86400` no `Dockerfile`).
- O banco fica no volume `s3-dados`: sobrevive a `down`, a `--build` e a reinícios.
- ROUTING e permissões de exemplo do A5 vêm na imagem. Para os da aplicação,
  descomente as linhas de `volumes` no `docker-compose.yml`.
- Parar: `docker compose -f infra/servidor/docker-compose.yml down`
  (o banco continua; `down -v` apaga o banco).

## Opção B — Python direto, com systemd

```bash
git clone https://github.com/ods-26-2/s3.git /opt/ods-s3
cd /opt/ods-s3
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
```

`/etc/systemd/system/ods-s3.service`:

```ini
[Unit]
Description=ODS S3 - Alertas e Notificações
After=network-online.target

[Service]
WorkingDirectory=/opt/ods-s3
Environment=PYTHONPATH=/opt/ods-s3/src
Environment=S3_KAFKA_BOOTSTRAP=172.18.4.56:9092
Environment=S3_KAFKA_GROUP_ID=S3-alertas
ExecStart=/opt/ods-s3/.venv/bin/python -m s3 --routing contratos/Barramentos/ROUTING.json --versao-routing routing-a5-1 --permissoes "contratos/Aplicações/Permissões de ação (exemplo).json" --banco /var/lib/ods-s3/s3.sqlite3 --tempo-limite 86400
Restart=always
RestartSec=10
StateDirectory=ods-s3

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload && sudo systemctl enable --now ods-s3
journalctl -u ods-s3 -f
```

Passo a passo em PDF, com o que já foi feito: [PASSO_A_PASSO_SERVIDOR_S3.pdf](PASSO_A_PASSO_SERVIDOR_S3.pdf)
(gerado por `tools/gerar_pdf_servidor.py`).

## Como conferir que está funcionando

1. No log, logo depois de ligar: `fila de saída` (se havia algo) e nenhum erro de conexão.
2. Mande um evento e veja no log `entrada schema=ods.s2.event ... resultado=alerta_criado`
   e `saida schema=ods.s3.alert ... state=CREATED`.
3. Pare e ligue de novo (`docker compose ... restart s3`): o alerta continua no banco
   e nada é reprocessado.

O `tools/kit_aplicacoes.py` e o `tools/teste_kafka_local.py` **não** servem para
este passo: os dois recusam qualquer broker que não seja `localhost`, de propósito.

## O que ainda precisa ser combinado

| Pergunta | Com quem |
|---|---|
| Qual servidor (máquina, sistema, acesso por SSH, se tem Docker) | quem pediu o S3 no servidor / P3 (SIS-1) |
| Os tópicos do S3 já existem no broker, ou o broker cria na primeira mensagem? | TRA-1 |
| Acesso à Jetson (usuário, SSH), se tem Docker e internet, e se cada squad roda o seu componente nela | professor / TRA-1 |
| ROUTING e permissões definitivos de cada aplicação | A3, A5, A7 |
