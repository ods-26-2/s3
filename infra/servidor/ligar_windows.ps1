# Liga o S3 num notebook ou PC com Windows e Docker Desktop, ligado ao broker dado.
# Para quando o S3 roda fora da Jetson: a máquina precisa estar na rede do broker
# (para o Kafka da Jetson, a rede do DComp) e ficar ligada enquanto o S3 for usado.
#
#   powershell -ExecutionPolicy Bypass -File infra\servidor\ligar_windows.ps1 172.18.4.56:9092
#
# Faz o mesmo que o instalar.sh (que é só para Linux), sem instalar nada:
#   1. confere o Docker Desktop;
#   2. guarda o broker em infra\servidor\.env (fora do git);
#   3. constrói a imagem, cria os tópicos do S3 que faltam e liga o ods-s3;
#   4. mostra o log e os comandos do dia a dia.
param(
    [Parameter(Mandatory = $true)]
    [string]$Broker
)

$ErrorActionPreference = "Continue"

function Passo($texto) { Write-Host ""; Write-Host "== $texto" }
function Parar($texto) { Write-Host ""; Write-Host "ERRO: $texto" -ForegroundColor Red; exit 1 }

if ($Broker -notmatch '^[^:\s]+:\d+$') { Parar "o broker deve ser host:porta, por exemplo 172.18.4.56:9092" }

$Raiz = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$Compose = Join-Path $Raiz "infra\servidor\docker-compose.yml"
Set-Location $Raiz

Passo "Docker"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    # O terminal do VS Code aberto antes da instalação não tem o PATH novo.
    $Bin = Join-Path $env:LOCALAPPDATA "Programs\DockerDesktop\resources\bin"
    if (-not (Test-Path (Join-Path $Bin "docker.exe"))) { $Bin = "C:\Program Files\Docker\Docker\resources\bin" }
    if (-not (Test-Path (Join-Path $Bin "docker.exe"))) { Parar "Docker não encontrado. Instale e abra o Docker Desktop." }
    $env:Path = "$Bin;$env:Path"
}
docker info --format "{{.ServerVersion}}" | Out-Null
if ($LASTEXITCODE -ne 0) { Parar "o Docker Desktop não está rodando. Abra-o e espere aparecer 'Engine running'." }
docker --version

Passo "Configuração (infra\servidor\.env)"
# Sem BOM: o docker compose leria o primeiro nome errado.
$Env = "S3_KAFKA_BOOTSTRAP=$Broker`nS3_KAFKA_GROUP_ID=S3-alertas`n"
[System.IO.File]::WriteAllText((Join-Path $Raiz "infra\servidor\.env"), $Env, (New-Object System.Text.UTF8Encoding($false)))
Write-Host $Env

Passo "Construindo a imagem"
docker compose -f $Compose build
if ($LASTEXITCODE -ne 0) { Parar "a imagem não foi construída (veja a mensagem acima; precisa de internet)." }

Passo "Tópicos do S3 no broker $Broker (cria os que faltam)"
docker run --rm --entrypoint python ods-s3 tools/criar_topicos.py $Broker
if ($LASTEXITCODE -ne 0) {
    Write-Host "AVISO: não foi possível conferir os tópicos. O S3 liga assim mesmo; confira a rede." -ForegroundColor Yellow
}

Passo "Ligando o ods-s3"
docker compose -f $Compose up -d --no-build
if ($LASTEXITCODE -ne 0) { Parar "o contêiner não ligou (veja a mensagem acima)." }
Start-Sleep -Seconds 5
docker compose -f $Compose ps

Passo "Últimas linhas do log"
docker compose -f $Compose logs --tail 20 s3

Write-Host ""
Write-Host "Pronto. O S3 fica ligado enquanto o Docker Desktop estiver aberto."
Write-Host "  verificar:   docker exec ods-s3 python tools/verificar_broker.py --broker-oficial"
Write-Host "  ver o log:   docker compose -f infra/servidor/docker-compose.yml logs -f s3"
Write-Host "  parar:       docker compose -f infra/servidor/docker-compose.yml down"
Write-Host "  ligar:       docker compose -f infra/servidor/docker-compose.yml up -d"
