#!/usr/bin/env bash
# Instala e liga o S3 num servidor Linux (Ubuntu/Debian), ligado ao broker dado.
#
#   git clone https://github.com/ods-26-2/s3.git && cd s3
#   bash infra/servidor/instalar.sh 34.66.235.91:9093
#
# Faz as etapas 5 a 7 do docs/PASSO_A_PASSO_SERVIDOR_S3.pdf:
#   1. instala o Docker, se faltar;
#   2. guarda o endereço do broker em infra/servidor/.env (fora do git), para
#      o `docker compose ... up -d` funcionar depois sem `export`;
#   3. constrói a imagem e liga o contêiner ods-s3;
#   4. confere se os tópicos do S3 existem no broker (só lê a lista);
#   5. mostra o log.
#
# Pode rodar de novo quando quiser: com --atualizar, faz `git pull` antes.
set -euo pipefail

uso() {
    echo "uso: bash infra/servidor/instalar.sh <broker host:porta> [--atualizar]" >&2
    exit 2
}

BROKER="${1:-}"
[[ "$BROKER" =~ ^[^:[:space:]]+:[0-9]+$ ]] || uso
ATUALIZAR="${2:-}"

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
"${DOCKER[@]}" compose version

passo "Configuração (infra/servidor/.env)"
cat > "$ENV" <<EOF
S3_KAFKA_BOOTSTRAP=$BROKER
S3_KAFKA_GROUP_ID=S3-alertas
EOF
cat "$ENV"

passo "Construindo a imagem e ligando o ods-s3"
"${DOCKER[@]}" compose -f "$COMPOSE" up -d --build
sleep 5
"${DOCKER[@]}" compose -f "$COMPOSE" ps

passo "Tópicos do S3 no broker $BROKER (só leitura)"
ESPERADOS=9
ENCONTRADOS=$("${DOCKER[@]}" exec ods-s3 python -c "
from confluent_kafka.admin import AdminClient
t = AdminClient({'bootstrap.servers': '$BROKER'}).list_topics(timeout=15).topics
achados = sorted(x for x in t if x.startswith(('ods.s2.', 'ods.s3.', 'ods.s4.')))
print('\n'.join(achados))
" || true)
if [[ -z "$ENCONTRADOS" ]]; then
    echo "AVISO: nenhum tópico do S3 no broker (ou o broker não respondeu)."
    echo "       O S3 fica ligado, mas não recebe nada até os tópicos existirem: fale com a TRA-1."
else
    echo "$ENCONTRADOS"
    N=$(echo "$ENCONTRADOS" | wc -l)
    if (( N < ESPERADOS )); then
        echo "AVISO: $N de $ESPERADOS tópicos. Os que faltam precisam ser criados (TRA-1)."
    else
        echo "ok: $N tópicos."
    fi
fi

passo "Últimas linhas do log"
"${DOCKER[@]}" compose -f "$COMPOSE" logs --tail 20 s3

cat <<EOF

Pronto. O S3 fica ligado e volta sozinho se a máquina reiniciar.
  ver o log:   ${DOCKER[*]} compose -f infra/servidor/docker-compose.yml logs -f s3
  parar:       ${DOCKER[*]} compose -f infra/servidor/docker-compose.yml down
  ligar:       ${DOCKER[*]} compose -f infra/servidor/docker-compose.yml up -d
  atualizar:   bash infra/servidor/instalar.sh $BROKER --atualizar
EOF
