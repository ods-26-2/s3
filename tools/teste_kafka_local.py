"""
Teste do S3 contra um Kafka de verdade, rodando na própria máquina.

    docker compose -f infra/kafka-local/docker-compose.yml up -d
    .venv\\Scripts\\python tools/teste_kafka_local.py            # fluxo S2 → S3 → A5
    .venv\\Scripts\\python tools/teste_kafka_local.py --queda    # + queda do broker e do S3

Sobe o próprio `python -m s3` (processo separado, com SQLite, ROUTING e
permissões) e faz o papel do S2, do S4 e do A5: publica nos tópicos e lê
o que o S3 publica, conferindo cada mensagem com tests/s3/contrato.py.

Só roda contra localhost: o broker de testes da TRA-1 é pago e
compartilhado pelos 15 squads. Os tópicos do S3 no Kafka local são
apagados e recriados a cada execução.

Não faz parte da suíte do pytest (precisa do Docker). O S2, o S4 e o
A5 aqui são simulados: não é a integração com as aplicações reais.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from uuid import uuid4

RAIZ = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(RAIZ / "src"), str(RAIZ / "tests" / "s3")]

from confluent_kafka import Consumer, KafkaException, Producer  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

import contrato  # noqa: E402
from fabricas import PASTA_JSONS, comando, ler_json, mensagem_evento  # noqa: E402
from s3.kafka_adapter import TOPICOS_ENTRADA  # noqa: E402

SAIDAS = (
    "ods.s3.alert",
    "ods.s3.alert.reply",
    "ods.s3.alert_action.reply",
    "ods.s3.alerts.reply",
)
ACAO = "ods.s3.alert_action.command"
COMPOSE = RAIZ / "infra" / "kafka-local" / "docker-compose.yml"
FUTURO = "2030-01-01T00:00:00.000Z"

# Só nos clientes do simulador: sem isto, a queda do broker enche a tela
# de avisos de reconexão. O log do S3 fica completo no arquivo da partida.
SILENCIO = {"log_level": 2}


def conferir_local(endereco: str) -> None:
    for item in endereco.split(","):
        host = item.strip().rsplit(":", 1)[0]
        if host not in ("localhost", "127.0.0.1"):
            sys.exit(
                f"Recusado: {endereco} não é local. Este teste só roda "
                "contra o Kafka do Docker (localhost)."
            )


def passo(texto: str) -> None:
    print(f"  ok  {texto}", flush=True)


# ----------------------------------------------------------------
# Broker


def esperar_broker(endereco: str, prazo_s: float = 90) -> None:
    fim = time.monotonic() + prazo_s
    while True:
        try:
            AdminClient({"bootstrap.servers": endereco, **SILENCIO}).list_topics(timeout=5)
            return
        except KafkaException:
            if time.monotonic() > fim:
                raise
            time.sleep(1)


def zerar_topicos(endereco: str) -> None:
    """Apaga e recria os tópicos do S3, com uma partição cada."""

    admin = AdminClient({"bootstrap.servers": endereco, **SILENCIO})
    todos = list(TOPICOS_ENTRADA) + list(SAIDAS)

    existentes = set(admin.list_topics(timeout=10).topics)
    apagar = [t for t in todos if t in existentes]
    if apagar:
        for futuro in admin.delete_topics(apagar, operation_timeout=30).values():
            futuro.result()

    # A exclusão termina em segundo plano: tenta criar até conseguir.
    fim = time.monotonic() + 60
    faltam = todos
    while faltam:
        futuros = admin.create_topics([NewTopic(t, 1, 1) for t in faltam])
        faltam = []
        for topico, futuro in futuros.items():
            try:
                futuro.result()
            except KafkaException:
                faltam.append(topico)
        if faltam:
            if time.monotonic() > fim:
                raise RuntimeError(f"não consegui recriar: {faltam}")
            time.sleep(1)


def comando_docker() -> str:
    achado = shutil.which("docker")
    if achado:
        return achado
    padrao = (
        Path(os.environ.get("LOCALAPPDATA", ""))
        / "Programs" / "DockerDesktop" / "resources" / "bin" / "docker.exe"
    )
    if padrao.exists():
        return str(padrao)
    sys.exit("docker não encontrado no PATH.")


def compose(acao: str) -> None:
    subprocess.run(
        [comando_docker(), "compose", "-f", str(COMPOSE), acao],
        check=True, capture_output=True,
    )


# ----------------------------------------------------------------
# S2, S4 e A5 simulados


class Aplicacoes:
    def __init__(self, endereco: str) -> None:
        self.produtor = Producer({"bootstrap.servers": endereco, **SILENCIO})
        self.consumidor = Consumer({
            "bootstrap.servers": endereco,
            "group.id": f"simulador-{uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            **SILENCIO,
        })
        self.consumidor.subscribe(list(SAIDAS))
        self.recebidas = {t: [] for t in SAIDAS}
        self.chaves = {}  # message_id → chave de partição

    def publicar(self, mensagem: dict) -> dict:
        erros = []
        self.produtor.produce(
            mensagem["schema"],
            json.dumps(mensagem).encode("utf-8"),
            on_delivery=lambda erro, _m: erro and erros.append(erro),
        )
        if self.produtor.flush(15) or erros:
            raise RuntimeError(f"não publicou {mensagem['schema']}: {erros}")
        return mensagem

    def _ler(self, intervalo: float) -> None:
        mensagem = self.consumidor.poll(intervalo)
        if mensagem is None or mensagem.error():
            return
        dados = json.loads(mensagem.value())
        self.recebidas[mensagem.topic()].append(dados)
        self.chaves[dados["message_id"]] = mensagem.key()

    def esperar(self, topico: str, condicao, prazo_s: float = 45) -> dict:
        fim = time.monotonic() + prazo_s
        while time.monotonic() < fim:
            for dados in self.recebidas[topico]:
                if condicao(dados):
                    return dados
            self._ler(0.5)
        raise AssertionError(f"nada chegou em {topico} em {prazo_s:.0f} s")

    def resposta(self, topico: str, pedido: dict) -> dict:
        return self.esperar(
            topico, lambda r: r.get("correlation_id") == pedido["message_id"]
        )

    def drenar(self, segundos: float) -> None:
        fim = time.monotonic() + segundos
        while time.monotonic() < fim:
            self._ler(0.5)

    def alertas(self, alerta_id: str | None = None) -> list[dict]:
        return [
            a for a in self.recebidas["ods.s3.alert"]
            if alerta_id is None or a["payload"]["alert_id"] == alerta_id
        ]

    def acao(self, alerta_id: str, action: str, actor: str, **extra) -> dict:
        return self.publicar(comando(
            ACAO, {"alert_id": alerta_id, "action": action, "actor": actor, **extra}
        ))

    def fechar(self) -> None:
        self.consumidor.close()


def novo_evento() -> dict:
    return mensagem_evento({"event_id": str(uuid4())})


def novo_clipe(event_id: str) -> dict:
    clipe = ler_json("clipe.json")
    clipe["message_id"] = str(uuid4())
    clipe["correlation_id"] = event_id
    clipe["payload"].update(clip_id=str(uuid4()), event_id=event_id)
    return clipe


# ----------------------------------------------------------------
# O S3, como processo separado


class ProcessoS3:
    def __init__(self, endereco: str, pasta: Path) -> None:
        self.endereco = endereco
        self.pasta = pasta
        self.grupo = f"S3-alertas-teste-{uuid4()}"
        self.partidas = 0
        self.processo = None

    def iniciar(self) -> None:
        self.partidas += 1
        self.log = self.pasta / f"s3-partida-{self.partidas}.log"
        ambiente = dict(
            os.environ,
            PYTHONPATH=str(RAIZ / "src"),
            PYTHONIOENCODING="utf-8",
            S3_KAFKA_BOOTSTRAP=self.endereco,
            S3_KAFKA_GROUP_ID=self.grupo,
        )
        self._arquivo = open(self.log, "w", encoding="utf-8")
        self.processo = subprocess.Popen(
            [
                sys.executable, "-m", "s3",
                "--routing", str(PASTA_JSONS / "routing_a5.json"),
                "--versao-routing", "routing-a5-1",
                "--permissoes", str(PASTA_JSONS / "permissoes_a5.json"),
                "--banco", str(self.pasta / "s3.sqlite3"),
                "--tempo-limite", "900",
            ],
            cwd=RAIZ, env=ambiente,
            stdout=self._arquivo, stderr=subprocess.STDOUT,
        )

    def derrubar(self) -> None:
        """Mata o processo sem aviso, como uma queda."""

        if self.processo and self.processo.poll() is None:
            self.processo.kill()
            self.processo.wait(15)
        if self.processo:
            self._arquivo.close()
        self.processo = None

    def vivo(self) -> bool:
        return self.processo is not None and self.processo.poll() is None


# ----------------------------------------------------------------
# Cenários


def fluxo(apps: Aplicacoes) -> dict:
    print("\n[1] Fluxo S2 → S3 → A5")

    evento = apps.publicar(novo_evento())
    event_id = evento["payload"]["event_id"]
    criado = apps.esperar(
        "ods.s3.alert", lambda a: a.get("correlation_id") == event_id
    )
    contrato.mensagem_alerta(criado)
    alerta_id = criado["payload"]["alert_id"]
    assert criado["payload"]["state"] == "CREATED"
    assert criado["session_id"] == evento["session_id"]
    assert criado["artifact_version"] == "routing-a5-1"
    assert apps.chaves[criado["message_id"]] == alerta_id.encode("utf-8")
    passo(f"S2 publicou o evento; S3 publicou o alerta CREATED (chave = alert_id {alerta_id[:8]}…)")

    clipe = apps.publicar(novo_clipe(event_id))
    clip_id = clipe["payload"]["clip_id"]
    com_clipe = apps.esperar(
        "ods.s3.alert", lambda a: a["payload"].get("clip_id") == clip_id
    )
    contrato.mensagem_alerta(com_clipe)
    assert com_clipe["payload"]["state"] == "CREATED"
    passo("S4 publicou o clipe; S3 republicou o alerta com o clip_id")

    ack = apps.acao(alerta_id, "ACKNOWLEDGE", "user:maria", reason="a caminho")
    negado = apps.acao(alerta_id, "MUTE", "user:ana", mute_until=FUTURO)
    mute = apps.acao(alerta_id, "MUTE", "user:maria", mute_until=FUTURO)
    close = apps.acao(alerta_id, "CLOSE", "user:maria", reason="EPI colocado")

    respostas = {}
    for pedido in (ack, negado, mute, close):
        resposta = apps.resposta("ods.s3.alert_action.reply", pedido)
        contrato.envelope(resposta, "RPL")
        respostas[pedido["message_id"]] = resposta["payload"]

    assert respostas[ack["message_id"]]["state"] == "ACKNOWLEDGED"
    assert respostas[negado["message_id"]]["error"]["code"] == "UNAUTHORIZED_ACTOR"
    assert respostas[mute["message_id"]]["state"] == "MUTED"
    assert respostas[close["message_id"]]["state"] == "CLOSED"
    passo("A5: ACK (maria), MUTE negado (ana → UNAUTHORIZED_ACTOR), MUTE e CLOSE (maria)")

    apps.esperar(
        "ods.s3.alert",
        lambda a: a["payload"]["alert_id"] == alerta_id and a["payload"]["state"] == "CLOSED",
    )
    versoes = apps.alertas(alerta_id)
    for versao in versoes:
        contrato.mensagem_alerta(versao)
    estados = [v["payload"]["state"] for v in versoes]
    sequencias = [v["sequence"] for v in versoes]
    assert estados == ["CREATED", "CREATED", "ACKNOWLEDGED", "MUTED", "CLOSED"], estados
    assert sequencias == sorted(sequencias) and len(set(sequencias)) == len(sequencias), sequencias
    assert len({v["message_id"] for v in versoes}) == len(versoes)
    passo(f"alerta republicado a cada mudança, em ordem: {' → '.join(estados)} (sequence {sequencias})")

    consulta = apps.publicar(comando("ods.s3.alerts.query", {"severity": "HIGH"}))
    historico = apps.resposta("ods.s3.alerts.reply", consulta)
    contrato.envelope(historico, "RPL")
    [item] = [a for a in historico["payload"]["alerts"] if a["alert_id"] == alerta_id]
    assert item["state"] == "CLOSED" and item["clip_id"] == clip_id
    assert item["acknowledged_by"] == "user:maria" and item["closed_by"] == "user:maria"
    passo("consulta ao histórico devolveu o alerta CLOSED, com clipe e autores")

    return evento


def queda(apps: Aplicacoes, s3: ProcessoS3, endereco: str, evento_antigo: dict) -> None:
    print("\n[2] Queda do broker com o S3 rodando")

    compose("stop")
    passo("broker parado (docker compose stop)")
    time.sleep(10)
    assert s3.vivo(), "o S3 caiu junto com o broker"
    passo("S3 continuou de pé durante 10 s sem broker")

    compose("start")
    esperar_broker(endereco)
    passo("broker de volta (docker compose start)")

    evento = apps.publicar(novo_evento())
    event_id = evento["payload"]["event_id"]
    alerta = apps.esperar(
        "ods.s3.alert", lambda a: a.get("correlation_id") == event_id, prazo_s=90
    )
    alerta_id = alerta["payload"]["alert_id"]
    passo("S3 reconectou sozinho: evento novo virou alerta, sem reiniciar o S3")

    print("\n[3] Queda do S3 (processo morto), com mensagens chegando")

    s3.derrubar()
    passo("S3 derrubado (kill)")

    evento3 = apps.publicar(novo_evento())
    ack = apps.acao(alerta_id, "ACKNOWLEDGE", "user:maria")
    apps.publicar(evento_antigo)  # o S2 reentrega um evento já processado
    passo("enquanto o S3 estava fora: evento novo, ACK e um evento repetido")

    s3.iniciar()
    passo("S3 iniciado de novo, com o mesmo banco e o mesmo grupo")

    event_id3 = evento3["payload"]["event_id"]
    apps.esperar("ods.s3.alert", lambda a: a.get("correlation_id") == event_id3, prazo_s=90)
    resposta = apps.resposta("ods.s3.alert_action.reply", ack)
    assert resposta["payload"]["state"] == "ACKNOWLEDGED", resposta["payload"]
    passo("nada se perdeu: o evento virou alerta e o ACK foi respondido")

    apps.drenar(8)
    antigo = evento_antigo["payload"]["event_id"]
    criados = [
        a for a in apps.alertas()
        if a.get("correlation_id") == antigo and a["payload"]["state"] == "CREATED"
        and a["payload"].get("clip_id") is None
    ]
    assert len(criados) == 1, f"evento repetido gerou {len(criados)} alertas"
    passo("o evento repetido não criou outro alerta")


def verificar_duplicadas(apps: Aplicacoes) -> None:
    ids = [a["message_id"] for a in apps.alertas()]
    repetidas = len(ids) - len(set(ids))
    assert repetidas == 0, f"{repetidas} ods.s3.alert repetida(s) pelo message_id"
    passo(f"{len(ids)} ods.s3.alert publicados, nenhum repetido")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument(
        "--queda", action="store_true",
        help="também derruba o broker e o S3 no meio do teste",
    )
    argumentos = parser.parse_args()
    endereco = argumentos.broker
    conferir_local(endereco)

    print(f"Kafka local: {endereco}")
    esperar_broker(endereco, prazo_s=15)
    zerar_topicos(endereco)
    passo("tópicos do S3 recriados")

    pasta = Path(tempfile.mkdtemp(prefix="s3-kafka-local-"))
    s3 = ProcessoS3(endereco, pasta)
    apps = Aplicacoes(endereco)
    broker_parado = False

    try:
        s3.iniciar()
        evento = fluxo(apps)
        if argumentos.queda:
            broker_parado = True
            queda(apps, s3, endereco, evento)
            broker_parado = False
        print()
        verificar_duplicadas(apps)
    except Exception as erro:
        print(f"\nFALHOU: {type(erro).__name__}: {erro}")
        print(f"Logs do S3 em {pasta}")
        return 1
    finally:
        s3.derrubar()
        apps.fechar()
        if broker_parado:
            compose("start")

    print(f"\nTudo certo. Logs do S3 (uma linha por mensagem) em {pasta}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
