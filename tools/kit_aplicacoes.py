"""
Kit para o backend de uma aplicação (A5, A3, A7) testar contra o S3
num Kafka local, sem o broker da TRA-1 e sem esperar o S2.

    docker compose -f infra/kafka-local/docker-compose.yml up -d
    .venv\\Scripts\\python tools/kit_aplicacoes.py preparar
    .venv\\Scripts\\python tools/kit_aplicacoes.py s3              # deixe rodando
    .venv\\Scripts\\python tools/kit_aplicacoes.py evento          # o S2 publica
    .venv\\Scripts\\python tools/kit_aplicacoes.py clipe <event_id>  # o S4 publica
    .venv\\Scripts\\python tools/kit_aplicacoes.py cenario D4      # testa o consumidor

Passo a passo em docs/KIT_APLICACOES_KAFKA_LOCAL.md. Só roda contra
localhost: o broker de testes é pago e compartilhado.
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

from teste_kafka_local import (
    PASTA_JSONS,
    RAIZ,
    conferir_local,
    esperar_broker,
    novo_clipe,
    zerar_topicos,
)

import contrato  # noqa: E402
from fabricas import FUTURO, PublicadorFalso, mensagem_evento, roteador  # noqa: E402
from s3.ingestion import IngestorEventos  # noqa: E402
from s3.kafka_adapter import PublicadorKafka  # noqa: E402
from s3.service import AlertService  # noqa: E402


# ----------------------------------------------------------------
# Cenários D1–D5 (docs/CHECKLIST_INTEGRACAO_S3.md, seção 2.4)
#
# As versões do alerta são geradas pelo próprio código do S3 e só
# então duplicadas ou embaralhadas: é exatamente o que o S3 publica.

CENARIOS = {
    "D1": (
        "a mesma mensagem CREATED entregue duas vezes (mesmo message_id)",
        ["CREATED", "CREATED"],
        "um único alerta, em CREATED; a segunda entrega é descartada",
    ),
    "D2": (
        "CREATED e depois ACKNOWLEDGED do mesmo alert_id",
        ["CREATED", "ACKNOWLEDGED"],
        "um único alerta, atualizado para ACKNOWLEDGED",
    ),
    "D3": (
        "ciclo completo, na ordem",
        ["CREATED", "ACKNOWLEDGED", "MUTED", "CLOSED"],
        "CLOSED, com acknowledged_by, muted_until e closed_by",
    ),
    "D4": (
        "CLOSED entregue antes de ACKNOWLEDGED e MUTED (sequence maior antes)",
        ["CREATED", "CLOSED", "ACKNOWLEDGED", "MUTED"],
        "continua CLOSED: ignora sequence menor do mesmo alert_id",
    ),
    "D5": (
        "alerta criado por comando, sem session_id e sem event_id",
        ["COMANDO"],
        "aceito; ordenado pelo sequence, que conta só pelo produtor S3",
    ),
}


def gerar_versoes(service: AlertService, publicador: PublicadorFalso) -> dict:
    """Um alerta de evento em CREATED → ACK → MUTED → CLOSED, como o S3 publica."""

    inicio = len(publicador.mensagens)
    IngestorEventos(service).receber(
        json.dumps(mensagem_evento({"event_id": str(uuid4())}))
    )
    alerta_id = publicador.mensagens[inicio]["payload"]["alert_id"]
    service.reconhecer_alerta(alerta_id, "user:maria", "a caminho")
    service.silenciar_alerta(alerta_id, "user:maria", FUTURO)
    service.encerrar_alerta(alerta_id, "user:maria", "EPI colocado")

    versoes = publicador.mensagens[inicio:]
    for versao in versoes:
        contrato.mensagem_alerta(versao)
    return {v["payload"]["state"]: v for v in versoes}


def gerar_de_comando(service: AlertService, publicador: PublicadorFalso) -> dict:
    service.criar_alerta(
        titulo="Inspeção de EPI pendente",
        mensagem="Turno da tarde sem inspeção registrada",
        severidade="MEDIUM",
        destinatarios=["role:supervisor"],
        canais=["SCREEN"],
        correlacao_id=str(uuid4()),
    )
    mensagem = publicador.mensagens[-1]
    contrato.mensagem_alerta(mensagem, origem="comando")
    return mensagem


def cenario(endereco: str, nomes: list[str]) -> None:
    publicador_falso = PublicadorFalso()
    service = AlertService(roteador=roteador(), publicador=publicador_falso)
    kafka = PublicadorKafka(endereco=endereco)

    for nome in nomes:
        descricao, ordem, esperado = CENARIOS[nome]

        if ordem == ["COMANDO"]:
            mensagens = [gerar_de_comando(service, publicador_falso)]
        else:
            versoes = gerar_versoes(service, publicador_falso)
            mensagens = [versoes[estado] for estado in ordem]

        for mensagem in mensagens:
            kafka.publicar(json.dumps(mensagem))

        alerta_id = mensagens[0]["payload"]["alert_id"]
        print(f"\n{nome}: {descricao}")
        for mensagem in mensagens:
            print(
                f"  publicado  state={mensagem['payload']['state']:<12} "
                f"sequence={mensagem['sequence']:<3} message_id={mensagem['message_id']}"
            )
        print(f"  alert_id   {alerta_id}")
        print(f"  esperado no backend: {esperado}")


# ----------------------------------------------------------------
# S3, S2 e S4


def rodar_s3(endereco: str, argumentos: argparse.Namespace) -> int:
    comando = [
        sys.executable, "-m", "s3",
        "--banco", argumentos.banco,
        "--tempo-limite", str(argumentos.tempo_limite),
    ]
    if not argumentos.sem_routing:
        comando += ["--routing", argumentos.routing, "--versao-routing", "routing-a5-1"]
    if argumentos.permissoes:
        comando += ["--permissoes", argumentos.permissoes]

    ambiente = dict(
        os.environ,
        PYTHONPATH=str(RAIZ / "src"),
        PYTHONIOENCODING="utf-8",
        S3_KAFKA_BOOTSTRAP=endereco,
    )
    print(f"S3 em {endereco}, banco {argumentos.banco}. Ctrl+C para parar.", flush=True)
    try:
        return subprocess.run(comando, cwd=RAIZ, env=ambiente).returncode
    except KeyboardInterrupt:
        return 0


def publicar(endereco: str, mensagem: dict) -> None:
    PublicadorKafka(endereco=endereco).publicar(json.dumps(mensagem))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--broker", default="localhost:9092")
    sub = parser.add_subparsers(dest="acao", required=True)

    preparar = sub.add_parser("preparar", help="cria os tópicos do S3 que faltam")
    preparar.add_argument(
        "--zerar", action="store_true",
        help="apaga e recria os tópicos (some tudo o que foi publicado)",
    )

    s3 = sub.add_parser("s3", help="roda o S3 ligado ao Kafka local")
    s3.add_argument("--routing", default=str(PASTA_JSONS / "routing_a5.json"))
    s3.add_argument("--sem-routing", action="store_true", help="como na build do A7")
    s3.add_argument("--permissoes", default=str(PASTA_JSONS / "permissoes_a5.json"))
    s3.add_argument("--banco", default=str(RAIZ / "s3-local.sqlite3"))
    s3.add_argument("--tempo-limite", type=float, default=3600)

    evento = sub.add_parser("evento", help="o S2 publica um ods.s2.event")
    evento.add_argument("--event-id", help="padrão: um novo")
    evento.add_argument("--fase", default="STARTED", choices=["STARTED", "UPDATED", "ENDED"])
    evento.add_argument("--status", default="CONFIRMED", choices=["CONFIRMED", "UNCERTAIN"])

    clipe = sub.add_parser("clipe", help="o S4 publica o ods.s4.clip de um evento")
    clipe.add_argument("event_id")

    cen = sub.add_parser("cenario", help="publica em ods.s3.alert os cenários D1–D5")
    cen.add_argument("nomes", nargs="*", help=f"{', '.join(CENARIOS)} (padrão: todos)")

    argumentos = parser.parse_args()
    if argumentos.acao == "cenario":
        desconhecidos = [n for n in argumentos.nomes if n not in CENARIOS]
        if desconhecidos:
            parser.error(f"cenário desconhecido: {', '.join(desconhecidos)}")

    endereco = argumentos.broker
    conferir_local(endereco)
    esperar_broker(endereco, prazo_s=15)

    if argumentos.acao == "preparar":
        criados = zerar_topicos(endereco, apagar=argumentos.zerar)
        print(f"tópicos criados: {', '.join(criados) or 'nenhum (já existiam)'}")
    elif argumentos.acao == "s3":
        zerar_topicos(endereco, apagar=False)
        return rodar_s3(endereco, argumentos)
    elif argumentos.acao == "evento":
        mensagem = mensagem_evento({
            "event_id": argumentos.event_id or str(uuid4()),
            "phase": argumentos.fase,
            "status": argumentos.status,
        })
        publicar(endereco, mensagem)
        print(f"ods.s2.event {argumentos.fase} {argumentos.status} publicado")
        print(f"event_id {mensagem['payload']['event_id']}")
    elif argumentos.acao == "clipe":
        mensagem = novo_clipe(argumentos.event_id)
        publicar(endereco, mensagem)
        print(f"ods.s4.clip publicado: clip_id {mensagem['payload']['clip_id']}")
    elif argumentos.acao == "cenario":
        cenario(endereco, argumentos.nomes or list(CENARIOS))

    return 0


if __name__ == "__main__":
    sys.exit(main())
