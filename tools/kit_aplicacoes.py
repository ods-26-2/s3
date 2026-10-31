"""
Kit para o backend de uma aplicação (A5, A3, A7) testar contra o S3
num Kafka local, sem o broker da TRA-1 e sem esperar o S2.

    docker compose -f infra/kafka-local/docker-compose.yml up -d
    .venv\\Scripts\\python tools/kit_aplicacoes.py preparar
    .venv\\Scripts\\python tools/kit_aplicacoes.py s3              # deixe rodando
    .venv\\Scripts\\python tools/kit_aplicacoes.py evento          # o S2 publica
    .venv\\Scripts\\python tools/kit_aplicacoes.py clipe <event_id>  # o S4 publica
    .venv\\Scripts\\python tools/kit_aplicacoes.py cenario D4      # testa o consumidor

Fazendo o papel do A5 (para demonstrar o S3 sem o backend real):

    .venv\\Scripts\\python tools/kit_aplicacoes.py aplicacao       # recebe os alertas
    .venv\\Scripts\\python tools/kit_aplicacoes.py acao <alert_id> ACKNOWLEDGE
    .venv\\Scripts\\python tools/kit_aplicacoes.py consulta        # histórico
    .venv\\Scripts\\python tools/kit_aplicacoes.py banco           # o que o S3 gravou

Passo a passo em docs/KIT_APLICACOES_KAFKA_LOCAL.md e, para uma
demonstração completa, docs/ROTEIRO_DEMONSTRACAO_S3.md. Só roda contra
localhost: o broker de testes é pago e compartilhado.
"""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from teste_kafka_local import (
    PASTA_JSONS,
    RAIZ,
    SILENCIO,
    conferir_local,
    esperar_broker,
    novo_clipe,
    zerar_topicos,
)

from confluent_kafka import Consumer  # noqa: E402

import contrato  # noqa: E402
from fabricas import FUTURO, PublicadorFalso, comando, mensagem_evento, roteador  # noqa: E402
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


# ----------------------------------------------------------------
# O papel da aplicação (A5): receber alertas, mandar comandos, consultar


def agora_iso(minutos: float = 0) -> str:
    instante = datetime.now(timezone.utc) + timedelta(minutes=minutos)
    return instante.strftime("%Y-%m-%dT%H:%M:%S.") + f"{instante.microsecond // 1000:03d}Z"


def consumidor(endereco: str, topicos: list[str], inicio: str = "earliest") -> Consumer:
    leitor = Consumer({
        "bootstrap.servers": endereco,
        "group.id": f"aplicacao-{uuid4()}",
        "auto.offset.reset": inicio,
        "enable.auto.commit": False,
        **SILENCIO,
    })
    leitor.subscribe(topicos)
    return leitor


def aplicacao(endereco: str, desde_inicio: bool) -> int:
    """
    Faz o papel do backend do A5: recebe ods.s3.alert e aplica as regras
    de consumo (DECISOES_S3.md, 17 e 18): descarta message_id repetido,
    ignora sequence menor do mesmo alert_id e atualiza pelo alert_id.
    """

    leitor = consumidor(
        endereco, ["ods.s3.alert"], "earliest" if desde_inicio else "latest"
    )
    vistas: set[str] = set()
    alertas: dict[str, dict] = {}   # alert_id → último payload aplicado
    ultima_seq: dict[str, int] = {}

    print("A5 ouvindo ods.s3.alert. Ctrl+C para parar.", flush=True)
    try:
        while True:
            mensagem = leitor.poll(1.0)
            if mensagem is None or mensagem.error():
                continue

            dados = json.loads(mensagem.value())
            payload = dados["payload"]
            alerta_id = payload["alert_id"]
            curto = alerta_id[:8]

            if dados["message_id"] in vistas:
                print(f"\n[A5] {curto} mensagem repetida (mesmo message_id): descartada", flush=True)
                continue
            vistas.add(dados["message_id"])

            if dados["sequence"] < ultima_seq.get(alerta_id, -1):
                print(
                    f"\n[A5] {curto} {payload['state']} com sequence {dados['sequence']} "
                    f"menor que {ultima_seq[alerta_id]}: ignorada (chegou fora de ordem)",
                    flush=True,
                )
                continue

            novo = alerta_id not in alertas
            anterior = alertas.get(alerta_id, {})
            alertas[alerta_id] = payload
            ultima_seq[alerta_id] = dados["sequence"]

            print(
                f"\n[A5] {'NOVO ALERTA' if novo else 'ATUALIZAÇÃO'} {alerta_id}\n"
                f"     state={payload['state']}  severity={payload['severity']}  "
                f"sequence={dados['sequence']}\n"
                f"     {payload['title']}: {payload['message']}\n"
                f"     para {', '.join(payload['recipients'])} | canais {', '.join(payload['channels'])}",
                flush=True,
            )
            for campo in ("clip_id", "acknowledged_by", "muted_until", "closed_by"):
                if payload.get(campo) and payload.get(campo) != anterior.get(campo):
                    print(f"     {campo}: {payload[campo]}", flush=True)

            if payload["state"] == "CREATED" and novo:
                print(f"     → o backend do A5 avisa por {', '.join(payload['channels'])}", flush=True)
            elif payload["state"] in ("ACKNOWLEDGED", "MUTED", "CLOSED") \
                    and anterior.get("state") != payload["state"]:
                print("     → o backend do A5 para o som e o push", flush=True)
    except KeyboardInterrupt:
        return 0
    finally:
        leitor.close()


def esperar_resposta(leitor: Consumer, pedido: dict, prazo_s: float = 30) -> dict:
    fim = time.monotonic() + prazo_s
    while time.monotonic() < fim:
        mensagem = leitor.poll(0.5)
        if mensagem is None or mensagem.error():
            continue
        dados = json.loads(mensagem.value())
        if dados.get("correlation_id") == pedido["message_id"]:
            return dados
    sys.exit(f"O S3 não respondeu em {prazo_s:.0f} s. Ele está rodando (kit_aplicacoes.py s3)?")


def pedir(endereco: str, schema: str, payload: dict) -> dict:
    """Manda um comando ou consulta como o A5 e devolve a resposta do S3."""

    leitor = consumidor(endereco, [schema.rsplit(".", 1)[0] + ".reply"])
    try:
        pedido = comando(schema, payload, published_at=agora_iso())
        publicar(endereco, pedido)
        print(f"A5 enviou {schema} (message_id {pedido['message_id']})", flush=True)
        return esperar_resposta(leitor, pedido)["payload"]
    finally:
        leitor.close()


def acao(endereco: str, argumentos: argparse.Namespace) -> int:
    payload = {
        "alert_id": argumentos.alert_id,
        "action": argumentos.action,
        "actor": argumentos.actor,
    }
    if argumentos.motivo:
        payload["reason"] = argumentos.motivo
    if argumentos.action == "MUTE":
        payload["mute_until"] = argumentos.ate or agora_iso(minutos=30)

    resposta = pedir(endereco, "ods.s3.alert_action.command", payload)
    if resposta["status"] == "OK":
        print(f"S3 respondeu OK: o alerta agora está {resposta['state']}")
        return 0
    erro = resposta["error"]
    print(f"S3 respondeu ERRO {erro['code']}: {erro['message']}")
    return 1


def consulta(endereco: str, argumentos: argparse.Namespace) -> int:
    payload = {"limit": argumentos.limite}
    if argumentos.estado:
        payload["state"] = argumentos.estado
    if argumentos.severidade:
        payload["severity"] = argumentos.severidade

    resposta = pedir(endereco, "ods.s3.alerts.query", payload)
    if resposta["status"] != "OK":
        print(f"S3 respondeu ERRO {resposta['error']['code']}: {resposta['error']['message']}")
        return 1

    alertas = resposta["alerts"]
    print(f"S3 devolveu {len(alertas)} alerta(s) do histórico:")
    for item in alertas:
        autores = ", ".join(
            f"{rotulo} {item[campo]}"
            for campo, rotulo in (("acknowledged_by", "ACK"), ("muted_until", "mudo até"),
                                  ("closed_by", "fechado por"))
            if item.get(campo)
        )
        print(
            f"  {item['alert_id'][:8]}  {item['state']:<12} {item['severity']:<6} {item['title']}"
            f"{'  | ' + autores if autores else ''}{'  | com clipe' if item.get('clip_id') else ''}"
        )
    if resposta.get("next_from_at"):
        print(f"  (há mais: next_from_at {resposta['next_from_at']})")
    return 0


# ----------------------------------------------------------------
# O banco do S3 (só leitura; pode rodar com o S3 ligado)


def banco(caminho: str) -> int:
    if not Path(caminho).exists():
        sys.exit(f"Banco não encontrado: {caminho}. O S3 já rodou com ele?")

    conexao = sqlite3.connect(f"file:{Path(caminho).as_posix()}?mode=ro", uri=True)
    try:
        alertas = [json.loads(d) for (d,) in conexao.execute("SELECT dados FROM alertas")]
        historico = [
            json.loads(d) for (d,) in conexao.execute("SELECT dados FROM historico ORDER BY ordem")
        ]
        (mensagens,) = conexao.execute("SELECT COUNT(*) FROM mensagens").fetchone()
        (pendentes,) = conexao.execute("SELECT COUNT(*) FROM saida").fetchone()
    finally:
        conexao.close()

    print(f"Banco do S3: {caminho}\n")
    print(f"ALERTAS ({len(alertas)})")
    for a in alertas:
        print(f"  {a['alerta_id'][:8]}  {a['estado']:<12} {a['severidade']:<6} {a['titulo']}")
        detalhes = [
            f"evento {a['evento_id'][:8]}" if a.get("evento_id") else "criado por comando",
            f"clipe {a['clip_id'][:8]}" if a.get("clip_id") else None,
            f"ACK por {a['reconhecido_por']}" if a.get("reconhecido_por") else None,
            f"mudo até {a['silenciado_ate']}" if a.get("silenciado_ate") else None,
            f"fechado por {a['encerrado_por']}" if a.get("encerrado_por") else None,
        ]
        print(f"            {' | '.join(d for d in detalhes if d)}")

    print(f"\nHISTÓRICO ({len(historico)} registros, em ordem)")
    for h in historico:
        print(
            f"  {h['registrado_em'][:19]}  {h['alerta_id'][:8]}  {h['acao']:<12} "
            f"{h.get('autor_id') or '-':<12} {h.get('motivo') or ''}"
        )

    print(f"\nMENSAGENS JÁ PROCESSADAS (deduplicação): {mensagens}")
    print(f"FILA DE SAÍDA (ainda não confirmadas pelo Kafka): {pendentes}")
    return 0


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
    evento.add_argument("--zona", help="zone_id (padrão: galpao-solda)")
    evento.add_argument("--equipamento", help="equipment_item (padrão: luva)")
    evento.add_argument("--duracao", type=int, help="segundos sem o EPI (padrão: 8)")

    clipe = sub.add_parser("clipe", help="o S4 publica o ods.s4.clip de um evento")
    clipe.add_argument("event_id")

    app = sub.add_parser("aplicacao", help="faz o papel do A5: recebe os alertas do S3")
    app.add_argument("--desde-inicio", action="store_true", help="mostra também os alertas antigos")

    act = sub.add_parser("acao", help="o A5 manda ACKNOWLEDGE, MUTE ou CLOSE")
    act.add_argument("alert_id")
    act.add_argument("action", choices=["ACKNOWLEDGE", "MUTE", "CLOSE"])
    act.add_argument("--actor", default="user:maria", help="padrão: user:maria (supervisora)")
    act.add_argument("--motivo", help="reason")
    act.add_argument("--ate", help="mute_until em ISO 8601 UTC (padrão: daqui a 30 min)")

    cons = sub.add_parser("consulta", help="o A5 pede o histórico de alertas")
    cons.add_argument("--estado", choices=["CREATED", "ACKNOWLEDGED", "MUTED", "CLOSED"])
    cons.add_argument("--severidade", choices=["LOW", "MEDIUM", "HIGH"])
    cons.add_argument("--limite", type=int, default=100)

    bd = sub.add_parser("banco", help="mostra o que o S3 gravou no SQLite (não precisa do Kafka)")
    bd.add_argument("--banco", default=str(RAIZ / "s3-local.sqlite3"))

    cen = sub.add_parser("cenario", help="publica em ods.s3.alert os cenários D1–D5")
    cen.add_argument("nomes", nargs="*", help=f"{', '.join(CENARIOS)} (padrão: todos)")

    argumentos = parser.parse_args()
    if argumentos.acao == "banco":
        return banco(argumentos.banco)

    if argumentos.acao == "cenario":
        desconhecidos = [n for n in argumentos.nomes if n not in CENARIOS]
        if desconhecidos:
            parser.error(f"cenário desconhecido: {', '.join(desconhecidos)}")

    endereco = argumentos.broker
    conferir_local(endereco)
    # Logo depois do `docker compose up`, o Kafka leva alguns segundos para atender.
    print(f"esperando o Kafka em {endereco}...", flush=True)
    try:
        esperar_broker(endereco, prazo_s=60)
    except Exception:
        sys.exit(
            f"O Kafka não respondeu em {endereco} em 60 s. Confira no Docker Desktop "
            "(Containers) se o s3-kafka-local está rodando."
        )

    if argumentos.acao == "preparar":
        criados = zerar_topicos(endereco, apagar=argumentos.zerar)
        print(f"tópicos criados: {', '.join(criados) or 'nenhum (já existiam)'}")
    elif argumentos.acao == "s3":
        zerar_topicos(endereco, apagar=False)
        return rodar_s3(endereco, argumentos)
    elif argumentos.acao == "evento":
        dados = {
            "event_id": argumentos.event_id or str(uuid4()),
            "phase": argumentos.fase,
            "status": argumentos.status,
        }
        if argumentos.zona:
            dados["zone_id"] = argumentos.zona
        if argumentos.equipamento:
            dados["equipment_item"] = argumentos.equipamento
        if argumentos.duracao is not None:
            dados["duration_ms"] = argumentos.duracao * 1000
        mensagem = mensagem_evento(dados, published_at=agora_iso(), captured_at=agora_iso())
        publicar(endereco, mensagem)
        p = mensagem["payload"]
        print(f"S2 publicou ods.s2.event {p['phase']} {p['status']}: sem {p['equipment_item']} "
              f"em {p['zone_id']} há {p['duration_ms'] // 1000} s")
        print(f"event_id {p['event_id']}")
    elif argumentos.acao == "clipe":
        mensagem = novo_clipe(argumentos.event_id)
        publicar(endereco, mensagem)
        print(f"ods.s4.clip publicado: clip_id {mensagem['payload']['clip_id']}")
    elif argumentos.acao == "aplicacao":
        return aplicacao(endereco, argumentos.desde_inicio)
    elif argumentos.acao == "acao":
        return acao(endereco, argumentos)
    elif argumentos.acao == "consulta":
        return consulta(endereco, argumentos)
    elif argumentos.acao == "cenario":
        cenario(endereco, argumentos.nomes or list(CENARIOS))

    return 0


if __name__ == "__main__":
    sys.exit(main())
