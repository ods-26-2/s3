# Liga o S3 no notebook, conversando com o Kafka da Jetson por uma ponte.
#
#   powershell -ExecutionPolicy Bypass -File infra\servidor\ligar_ponte_jetson.ps1
#   powershell -ExecutionPolicy Bypass -File infra\servidor\ligar_ponte_jetson.ps1 -Parar
#
# Por que a ponte: a porta 9092 do Kafka da Jetson é para quem está dentro da
# Jetson e anuncia localhost:9092. Fora dela, o cliente é mandado para o próprio
# localhost e não conecta. A ponte (socat) escuta em localhost:9092 e repassa
# para a Jetson; o S3 usa a rede da ponte, então o localhost dele leva à Jetson.
# Funciona porque o Kafka da Jetson tem um broker só. Precisa estar na rede do
# DComp. Quando a TRA-1 abrir uma porta de clientes externa, use o
# ligar_windows.ps1 com ela; dentro da Jetson, infra/jetson/docker-compose.yml.
#
# Passos: confere o Docker e a Jetson; liga a ponte; recusa se já houver um S3
# ligado no broker (dois S3 respondem aos mesmos comandos); cria os tópicos que
# faltam; liga o S3; roda a verificação.
param(
    [string]$Jetson = "172.18.4.56:9092",
    [switch]$Parar
)

$ErrorActionPreference = "Continue"
# Os contêineres escrevem em UTF-8; sem isto, o console mostra "t├│picos".
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

function Passo($texto) { Write-Host ""; Write-Host "== $texto" }
function Parar($texto) { Write-Host ""; Write-Host "ERRO: $texto" -ForegroundColor Red; exit 1 }

$Raiz = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $Raiz

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    # O terminal do VS Code aberto antes da instalação não tem o PATH novo.
    $Bin = Join-Path $env:LOCALAPPDATA "Programs\DockerDesktop\resources\bin"
    if (-not (Test-Path (Join-Path $Bin "docker.exe"))) { $Bin = "C:\Program Files\Docker\Docker\resources\bin" }
    if (-not (Test-Path (Join-Path $Bin "docker.exe"))) { Parar "Docker não encontrado. Instale e abra o Docker Desktop." }
    $env:Path = "$Bin;$env:Path"
}

function Desligar {
    # docker stop: o S3 sai do grupo na hora (SIGTERM); rm apaga o contêiner.
    foreach ($nome in "ods-s3", "ponte-jetson") {
        docker container inspect $nome *> $null
        if ($LASTEXITCODE -eq 0) {
            docker stop $nome | Out-Null
            docker rm $nome | Out-Null
            Write-Host "desligado: $nome"
        }
    }
}

if ($Parar) { Passo "Desligando o S3 e a ponte"; Desligar; exit 0 }

if ($Jetson -notmatch '^([^:\s]+):(\d+)$') { Parar "o endereço deve ser host:porta, por exemplo 172.18.4.56:9092" }
$Host_, $Porta = $Matches[1], $Matches[2]

Passo "Docker"
docker info --format "{{.ServerVersion}}" | Out-Null
if ($LASTEXITCODE -ne 0) { Parar "o Docker Desktop não está rodando. Abra-o e espere aparecer 'Engine running'." }

Passo "Jetson ($Jetson)"
$teste = Test-NetConnection $Host_ -Port $Porta -WarningAction SilentlyContinue
if (-not $teste.TcpTestSucceeded) {
    Parar "a porta $Porta de $Host_ não respondeu. Está na rede do DComp? O Kafka da Jetson está no ar (pergunte à TRA-1)?"
}
Write-Host "a porta $Porta responde"

Passo "Desligando o que sobrou de antes"
Desligar
docker compose -f infra/servidor/docker-compose.yml down *> $null
docker compose -f infra/jetson/docker-compose.yml down *> $null

Passo "Imagem do S3"
docker build -q -t ods-s3 . | Out-Null
if ($LASTEXITCODE -ne 0) { Parar "a imagem não foi construída (veja a mensagem acima)." }
Write-Host "imagem ods-s3 pronta"

Passo "Endereço que o Kafka da Jetson anuncia"
# A primeira conversa vai direto à Jetson e funciona; o Kafka responde com o
# endereço que os clientes devem usar dali em diante (localhost:9092 até 06/11,
# kafka:9092 em 11/11). A ponte escuta nessa porta, e esse nome passa a
# apontar para ela, dentro dos contêineres que usam a rede da ponte.
$anuncio = docker run --rm --entrypoint python ods-s3 -c "from confluent_kafka.admin import AdminClient; b = list(AdminClient({'bootstrap.servers': '$Jetson', 'log_level': 2}).list_topics(timeout=15).brokers.values())[0]; print(f'{b.host}:{b.port}')"
if ($LASTEXITCODE -ne 0 -or $anuncio -notmatch '^([^:\s]+):(\d+)$') { Parar "o Kafka da Jetson não respondeu ao pedido de metadados." }
$NomeAnunciado, $PortaAnunciada = $Matches[1], $Matches[2]
Write-Host "o Kafka anuncia: $anuncio"

Passo "Ponte para a Jetson"
$Mapa = @()
if ($NomeAnunciado -notin @("localhost", "127.0.0.1")) { $Mapa = @("--add-host", "${NomeAnunciado}:127.0.0.1") }
docker run -d --name ponte-jetson @Mapa alpine/socat "TCP-LISTEN:${PortaAnunciada},fork,reuseaddr" "TCP:${Host_}:${Porta}" | Out-Null
if ($LASTEXITCODE -ne 0) { Parar "a ponte não ligou (precisa baixar a imagem alpine/socat na primeira vez)." }
Start-Sleep -Seconds 2
Write-Host "ponte ligada: ${NomeAnunciado}:${PortaAnunciada} -> $Jetson"

Passo "Já há um S3 ligado no broker?"
docker run --rm --network container:ponte-jetson --entrypoint python ods-s3 tools/outro_s3.py localhost:${PortaAnunciada}
$codigo = $LASTEXITCODE
if ($codigo -eq 1) {
    Desligar
    Parar "já há um S3 ligado neste Kafka (talvez o que a equipe de implantação subiu na Jetson). Não ligue outro: confira com eles. Se for um S3 seu que acabou de cair, espere 1 minuto e rode de novo."
}
if ($codigo -ne 0) { Desligar; Parar "não foi possível consultar o Kafka da Jetson pela ponte." }

Passo "Tópicos do S3 (cria os que faltam)"
docker run --rm --network container:ponte-jetson --entrypoint python ods-s3 tools/criar_topicos.py localhost:${PortaAnunciada} | Select-Object -Last 1

Passo "Ligando o S3"
docker run -d --name ods-s3 --restart unless-stopped --network container:ponte-jetson -e S3_KAFKA_BOOTSTRAP=localhost:${PortaAnunciada} -v ods-s3-dados:/dados ods-s3 | Out-Null
if ($LASTEXITCODE -ne 0) { Desligar; Parar "o S3 não ligou (veja a mensagem acima)." }
Write-Host "S3 ligado; esperando ele se conectar..."
Start-Sleep -Seconds 12

Passo "Verificação pelo Kafka da Jetson"
docker run --rm --network container:ponte-jetson --entrypoint python -e S3_KAFKA_BOOTSTRAP=localhost:${PortaAnunciada} ods-s3 tools/verificar_broker.py --broker-oficial --prazo 90

Write-Host ""
Write-Host "O S3 fica ligado (pela ponte) enquanto o notebook estiver na rede do DComp e o Docker aberto."
Write-Host "  ver o log:   docker logs -f ods-s3"
Write-Host "  verificar:   docker run --rm --network container:ponte-jetson --entrypoint python -e S3_KAFKA_BOOTSTRAP=localhost:9092 ods-s3 tools/verificar_broker.py --broker-oficial"
Write-Host "  desligar:    powershell -ExecutionPolicy Bypass -File infra\servidor\ligar_ponte_jetson.ps1 -Parar"
