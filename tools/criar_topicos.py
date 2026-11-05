"""
Cria no broker os tópicos do S3 que ainda não existem (vai dentro da imagem).

    docker run --rm --entrypoint python ods-s3 tools/criar_topicos.py 172.18.4.56:9092

Usado pelo infra/servidor/instalar.sh e pelo infra/servidor/ligar_windows.ps1,
antes de ligar o S3: um tópico criado depois que o S3 já está escutando pode
levar até 5 min para ser percebido (intervalo padrão de metadados do cliente).
A TRA-1 liberou os squads para criar os tópicos (05/11/2026). Partições e
réplicas ficam no padrão do broker. Tópico que já existe não é tocado.

Saída 0: os 9 tópicos existem. Saída 1: o broker não respondeu ou algum
tópico não pôde ser criado.
"""

import sys

from confluent_kafka.admin import AdminClient, NewTopic

TOPICOS = [
    "ods.s2.event", "ods.s4.clip",
    "ods.s3.alert.command", "ods.s3.alert_action.command", "ods.s3.alerts.query",
    "ods.s3.alert", "ods.s3.alert.reply", "ods.s3.alert_action.reply", "ods.s3.alerts.reply",
]


def main() -> int:
    if len(sys.argv) != 2:
        print("uso: python tools/criar_topicos.py <broker host:porta>", file=sys.stderr)
        return 2

    admin = AdminClient({"bootstrap.servers": sys.argv[1], "log_level": 2})
    try:
        existentes = admin.list_topics(timeout=15).topics
    except Exception as erro:
        print(f"AVISO: o broker {sys.argv[1]} não respondeu ({erro}).")
        print("       Confira se esta máquina está na rede do broker (DComp, para a Jetson).")
        return 1

    faltam = [t for t in TOPICOS if t not in existentes]
    falhas = 0
    for topico, futuro in (admin.create_topics([NewTopic(t, -1, -1) for t in faltam]).items() if faltam else []):
        try:
            futuro.result()
            print("criado   ", topico)
        except Exception as erro:
            falhas += 1
            print("AVISO    ", topico, erro)
    for topico in TOPICOS:
        if topico in existentes:
            print("já existe", topico)

    if falhas:
        return 1
    print(f"ok: {len(TOPICOS)} tópicos do S3 conferidos")
    return 0


if __name__ == "__main__":
    sys.exit(main())
