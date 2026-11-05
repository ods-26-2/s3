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
#   3. constrói a imagem;
#   4. cria no broker os tópicos do S3 que faltarem (liberado pela TRA-1);
#   5. liga o contêiner ods-s3 e mostra o log.
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

passo "Construindo a imagem"
"${DOCKER[@]}" compose -f "$COMPOSE" build

# Antes de ligar o S3: um tópico criado depois que ele já está escutando
# pode levar até 5 min para ser percebido. A TRA-1 liberou criar (05/11).
passo "Tópicos do S3 no broker $BROKER (cria os que faltam)"
"${DOCKER[@]}" run --rm --entrypoint python ods-s3 -c "
import sys
from confluent_kafka.admin import AdminClient, NewTopic
TOPICOS = ['ods.s2.event', 'ods.s4.clip', 'ods.s3.alert.command', 'ods.s3.alert_action.command',
           'ods.s3.alerts.query', 'ods.s3.alert', 'ods.s3.alert.reply', 'ods.s3.alert_action.reply',
           'ods.s3.alerts.reply']
admin = AdminClient({'bootstrap.servers': '$BROKER'})
try:
    existentes = admin.list_topics(timeout=15).topics
except Exception as erro:
    sys.exit(f'AVISO: o broker não respondeu ({erro}). O S3 liga, mas confira a rede do servidor.')
faltam = [t for t in TOPICOS if t not in existentes]
# -1: partições e réplicas no padrão do broker.
for topico, futuro in (admin.create_topics([NewTopic(t, -1, -1) for t in faltam]).items() if faltam else []):
    try:
        futuro.result()
        print('criado  ', topico)
    except Exception as erro:
        print('AVISO   ', topico, erro)
for topico in TOPICOS:
    if topico in existentes:
        print('já existe', topico)
print(f'ok: {len(TOPICOS)} tópicos do S3 conferidos')
" || true

passo "Ligando o ods-s3"
"${DOCKER[@]}" compose -f "$COMPOSE" up -d
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
