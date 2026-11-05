#!/usr/bin/env bash
# Instala e liga o S3 num servidor Linux (Ubuntu/Debian), ligado ao broker dado.
#
#   git clone https://github.com/ods-26-2/s3.git && cd s3
#   bash infra/servidor/instalar.sh 172.18.4.56:9092      # Kafka da Jetson (rede do DComp)
#
# Faz as etapas 5 a 7 do docs/PASSO_A_PASSO_SERVIDOR_S3.pdf:
#   1. instala o Docker, se faltar;
#   2. guarda o endereço do broker em infra/servidor/.env (fora do git), para
#      o `docker compose ... up -d` funcionar depois sem `export`;
#   3. constrói a imagem;
#   4. cria no broker os tópicos do S3 que faltarem (liberado pela TRA-1);
#   5. liga o contêiner ods-s3 e mostra o log.
#
# Pode rodar de novo quando quiser: com --atualizar, faz `git pull` antes.
# Sem internet na máquina, carregue antes a imagem pronta (docker load) e use
# --sem-construir: veja docs/IMPLANTACAO_SERVIDOR.md, "Sem internet".
set -euo pipefail

uso() {
    echo "uso: bash infra/servidor/instalar.sh <broker host:porta> [--atualizar] [--sem-construir]" >&2
    exit 2
}

BROKER="${1:-}"
[[ "$BROKER" =~ ^[^:[:space:]]+:[0-9]+$ ]] || uso

# Só no servidor (Jetson/Linux). No Windows (Git Bash), o sudo abre a janela
# de administrador e tentaria instalar o Docker na máquina errada.
if [[ "$(uname -s)" != "Linux" ]]; then
    echo "Este script roda no servidor Linux (a Jetson), não nesta máquina ($(uname -s))." >&2
    echo "Entre na Jetson (ssh USUARIO@IP_DA_JETSON, de dentro da rede do DComp) e rode lá." >&2
    exit 1
fi
shift
ATUALIZAR=""
CONSTRUIR=1
for opcao in "$@"; do
    case "$opcao" in
        --atualizar) ATUALIZAR="--atualizar" ;;
        --sem-construir) CONSTRUIR=0 ;;
        *) uso ;;
    esac
done

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE="$RAIZ/infra/servidor/docker-compose.yml"
ENV="$RAIZ/infra/servidor/.env"
cd "$RAIZ"

passo() { echo; echo "== $*"; }

if [[ "$ATUALIZAR" == "--atualizar" ]]; then
    passo "Atualizando o código (git pull)"
    git pull --ff-only
fi

passo "Docker"
if ! command -v docker >/dev/null 2>&1; then
    echo "Docker não encontrado; instalando (get.docker.com)..."
    curl -fsSL https://get.docker.com | sudo sh
    sudo usermod -aG docker "${USER:-$(id -un)}" || true
fi
# Logo depois da instalação, o grupo docker só vale num login novo: usa sudo.
if docker info >/dev/null 2>&1; then
    DOCKER=(docker)
else
    DOCKER=(sudo docker)
fi
"${DOCKER[@]}" --version
echo "arquitetura: $(uname -m)"   # Jetson: aarch64; a imagem é construída aqui, para ela

# Docker já instalado sem o plugin compose (comum na Jetson, com o docker.io
# do Ubuntu): tenta o pacote do Ubuntu e depois o do repositório do Docker.
if ! "${DOCKER[@]}" compose version >/dev/null 2>&1; then
    echo "docker compose não encontrado; instalando o plugin..."
    sudo apt-get update -qq || true
    sudo apt-get install -y docker-compose-v2 >/dev/null 2>&1 \
        || sudo apt-get install -y docker-compose-plugin >/dev/null 2>&1 \
        || true
fi
if ! "${DOCKER[@]}" compose version; then
    echo "ERRO: falta o docker compose. Instale o pacote docker-compose-v2 (Ubuntu)" >&2
    echo "      ou docker-compose-plugin (repositório do Docker) e rode de novo." >&2
    exit 1
fi

passo "Configuração (infra/servidor/.env)"
cat > "$ENV" <<EOF
S3_KAFKA_BOOTSTRAP=$BROKER
S3_KAFKA_GROUP_ID=S3-alertas
EOF
cat "$ENV"

passo "Construindo a imagem"
if (( CONSTRUIR )); then
    "${DOCKER[@]}" compose -f "$COMPOSE" build
elif "${DOCKER[@]}" image inspect ods-s3 >/dev/null 2>&1; then
    echo "--sem-construir: usando a imagem ods-s3 já carregada"
else
    echo "ERRO: --sem-construir, mas a imagem ods-s3 não está carregada (docker load -i ods-s3-arm64.tar)." >&2
    exit 1
fi

# Antes de ligar o S3: um tópico criado depois que ele já está escutando
# pode levar até 5 min para ser percebido. A TRA-1 liberou criar (05/11).
passo "Tópicos do S3 no broker $BROKER (cria os que faltam)"
"${DOCKER[@]}" run --rm --entrypoint python ods-s3 tools/criar_topicos.py "$BROKER" || true

passo "Ligando o ods-s3"
"${DOCKER[@]}" compose -f "$COMPOSE" up -d --no-build
sleep 5
"${DOCKER[@]}" compose -f "$COMPOSE" ps

passo "Últimas linhas do log"
"${DOCKER[@]}" compose -f "$COMPOSE" logs --tail 20 s3

cat <<EOF

Pronto. O S3 fica ligado e volta sozinho se a máquina reiniciar.
  verificar:   ${DOCKER[*]} exec ods-s3 python tools/verificar_broker.py --broker-oficial
               (uma vez: 4 comandos e 1 consulta, ~5 s; tire a captura)
  ver o log:   ${DOCKER[*]} compose -f infra/servidor/docker-compose.yml logs -f s3
  parar:       ${DOCKER[*]} compose -f infra/servidor/docker-compose.yml down
  ligar:       ${DOCKER[*]} compose -f infra/servidor/docker-compose.yml up -d
  atualizar:   bash infra/servidor/instalar.sh $BROKER --atualizar
EOF
