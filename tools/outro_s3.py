"""
Diz se já há um S3 ligado neste broker (vai dentro da imagem).

    docker run --rm --entrypoint python ods-s3 tools/outro_s3.py <broker>

Um S3 ligado aparece como o grupo de consumidores S3-alertas com membros
ativos. Dois S3 no mesmo broker respondem aos mesmos comandos, cada um com
o seu banco: antes de ligar outro (por exemplo, no notebook, pela ponte),
confira. Um S3 que acabou de cair ainda aparece ativo por uns 45 s.

Saída 0: nenhum S3 ligado. Saída 1: há um S3 ligado. Saída 2: erro.
"""

import os
import sys

from confluent_kafka import ConsumerGroupState
from confluent_kafka.admin import AdminClient

GRUPO = os.environ.get("S3_KAFKA_GROUP_ID", "S3-alertas")
ATIVOS = {
    ConsumerGroupState.STABLE,
    ConsumerGroupState.PREPARING_REBALANCING,
    ConsumerGroupState.COMPLETING_REBALANCING,
}


def main() -> int:
    if len(sys.argv) != 2:
        print("uso: python tools/outro_s3.py <broker host:porta>", file=sys.stderr)
        return 2

    admin = AdminClient({"bootstrap.servers": sys.argv[1], "log_level": 2})
    try:
        grupos = admin.list_consumer_groups(states=ATIVOS, request_timeout=15).result()
    except Exception as erro:
        print(f"não foi possível consultar o broker: {erro}")
        return 2

    if any(g.group_id == GRUPO for g in grupos.valid):
        print(f"há um S3 ligado neste broker (grupo {GRUPO} ativo)")
        return 1

    print(f"nenhum S3 ligado neste broker (grupo {GRUPO})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
