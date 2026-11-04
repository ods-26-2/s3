# Implantação do S3 num servidor

Como deixar o S3 rodando fora da máquina de desenvolvimento, ligado ao broker
do projeto (Guia do Kafka da TRA-1: `34.66.235.91:9093`, sem autenticação,
endereço temporário para testes).

## O que o S3 precisa

| Item | Valor |
|---|---|
| Sistema | Linux (ou qualquer um com Docker). Testado em Windows 11 |
| Python | 3.10 ou mais novo, se rodar sem Docker |
| Dependência | só `confluent-kafka` (`requirements.txt`) |
| Rede | saída para `34.66.235.91:9093`. O S3 não abre porta: não precisa de entrada |
| Disco | uma pasta com escrita para o SQLite (alertas, histórico, fila de saída); poucos MB |
| Memória e CPU | pouca: um processo Python; parado, o laço só espera 1 s por mensagem |
| Configuração | `S3_KAFKA_BOOTSTRAP` (obrigatória), `S3_KAFKA_GROUP_ID` (padrão `S3-alertas`), o arquivo de ROUTING e o de permissões |

**Uma instância só.** Com o S3 no servidor, não rode outro S3 contra o mesmo
broker (nem na sua máquina): dois S3 respondem aos mesmos comandos, cada um
com o seu banco, e a aplicação recebe respostas contraditórias.

## Opção A — Docker (recomendada)

No servidor, com o repositório clonado (`git clone https://github.com/ods-26-2/s3.git`):

```bash
export S3_KAFKA_BOOTSTRAP=34.66.235.91:9093
docker compose -f infra/servidor/docker-compose.yml up -d --build
docker compose -f infra/servidor/docker-compose.yml logs -f s3
```

PowerShell: `$env:S3_KAFKA_BOOTSTRAP = "34.66.235.91:9093"` no lugar do `export`.

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
Environment=S3_KAFKA_BOOTSTRAP=34.66.235.91:9093
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
| Pode deixar o S3 ligado o tempo todo no broker pago? | TRA-1 |
| ROUTING e permissões definitivos de cada aplicação | A3, A5, A7 |
