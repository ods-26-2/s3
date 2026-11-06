"""
Verificação mínima do S3 num broker real, para rodar uma vez depois da
instalação no servidor (S3.2: comunicação pelo barramento B3).

    # no servidor, com o ods-s3 ligado:
    docker exec ods-s3 python tools/verificar_broker.py --broker-oficial

Faz o papel do A5 (aplicação do VER-3), com 4 comandos e 1 consulta:
    1. lista os tópicos do S3 no broker (só leitura);
    2. cria um alerta por comando, com título "[TESTE S3]" (ods.s3.alert.command);
    3. espera o ods.s3.alert publicado pelo S3;
    4. ACKNOWLEDGE e CLOSE como user:maria, para o alerta de teste não
       ficar aberto nas aplicações (ods.s3.alert_action.command);
    5. consulta o histórico (ods.s3.alerts.query).

Não publica em ods.s2.event: não se passa pelo S2 num broker compartilhado.
O caminho S2 → S3 é verificado quando o S2 publicar de verdade.

O broker vem de --broker ou de S3_KAFKA_BOOTSTRAP. Fora de localhost, só
roda com --broker-oficial: o broker do projeto (Jetson, ou a nuvem de testes)
é compartilhado por todos os squads.
Os consumidores leem os tópicos do começo e só consideram as mensagens deste
teste (pelo alert_id e pelo correlation_id); tudo
termina em --prazo segundos.
"""

import argparse
import json
import os
import sys
import time
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from s3.envelope import agora, formatar_horario  # noqa: E402
from s3.kafka_adapter import TOPICOS_COMANDOS, TOPICOS_EVENTOS  # noqa: E402

RESPOSTAS = ("ods.s3.alert.reply", "ods.s3.alert_action.reply", "ods.s3.alerts.reply")
TOPICOS_S3 = TOPICOS_EVENTOS + TOPICOS_COMANDOS + ("ods.s3.alert",) + RESPOSTAS
SILENCIO = {"log_level": 2}
ACTOR = "user:maria"  # supervisora no arquivo de permissões de exemplo do A5


class Falhou(Exception):
    pass


def ok(texto: str) -> None:
    print(f"  ok  {texto}", flush=True)


def comando(schema: str, payload: dict) -> dict:
    return {
        "message_type": "query" if schema.endswith(".query") else "command",
        "message_id": str(uuid4()),
        "schema": schema,
        "schema_version": "1.0",
        "producer": "A5",
        "published_at": formatar_horario(agora()),
        "target": "S3",
        "payload": payload,
    }


class Verificador:
    def __init__(self, endereco: str, prazo_s: float) -> None:
        self.endereco = endereco
        self.fim = time.monotonic() + prazo_s
        self.produtor = Producer({"bootstrap.servers": endereco, **SILENCIO})
        self.leitor = Consumer({
            "bootstrap.servers": endereco,
            "group.id": f"S3-verificacao-{uuid4()}",
            # Do começo: com "latest", num broker remoto (Jetson, 06/11/2026) o
            # fim do tópico era marcado depois que o S3 já tinha publicado o
            # alerta, e a verificação o perdia. As mensagens são filtradas pelo
            # alert_id e pelo correlation_id deste teste; as antigas não contam.
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            **SILENCIO,
        })
        self.recebidas: list[tuple[str, dict]] = []
        self.enviadas = 0

    def restante(self) -> float:
        return self.fim - time.monotonic()

    def assinar(self) -> None:
        atribuido = []
        self.leitor.subscribe(
            ["ods.s3.alert", *RESPOSTAS],
            on_assign=lambda _c, particoes: atribuido.append(particoes),
        )
        # Com "latest", só publicar depois que as partições foram atribuídas.
        while not atribuido:
            if self.restante() <= 0:
                raise Falhou("o broker não atribuiu as partições a tempo")
            self.leitor.poll(0.5)

    def publicar(self, mensagem: dict) -> dict:
        erros = []
        self.produtor.produce(
            mensagem["schema"], json.dumps(mensagem).encode("utf-8"),
            on_delivery=lambda erro, _m: erro and erros.append(erro),
        )
        if self.produtor.flush(15) or erros:
            raise Falhou(f"o broker não aceitou {mensagem['schema']}: {erros or 'sem confirmação'}")
        self.enviadas += 1
        return mensagem

    def esperar(self, topico: str, condicao, o_que: str) -> dict:
        while self.restante() > 0:
            for recebido_em, dados in self.recebidas:
                if recebido_em == topico and condicao(dados):
                    return dados
            mensagem = self.leitor.poll(min(1.0, max(self.restante(), 0.1)))
            if mensagem is None or mensagem.error():
                continue
            self.recebidas.append((mensagem.topic(), json.loads(mensagem.value())))
        raise Falhou(f"não chegou {o_que} em {topico}. O S3 está ligado e lendo este broker?")

    def resposta(self, topico: str, pedido: dict) -> dict:
        return self.esperar(
            topico, lambda r: r.get("correlation_id") == pedido["message_id"],
            f"a resposta ao {pedido['schema']}",
        )["payload"]

    def fechar(self) -> None:
        self.leitor.close()


def acao(v: Verificador, alerta_id: str, action: str, esperado: str) -> None:
    pedido = v.publicar(comando("ods.s3.alert_action.command", {
        "alert_id": alerta_id, "action": action, "actor": ACTOR, "reason": "verificação do broker",
    }))
    payload = v.resposta("ods.s3.alert_action.reply", pedido)
    if payload.get("state") != esperado:
        raise Falhou(f"{action}: esperava {esperado}, veio {payload.get('state') or payload.get('error')}")
    ok(f"{action} de {ACTOR}: S3 respondeu {esperado}")


def verificar(v: Verificador) -> None:
    print("\n[1] Tópicos do S3 no broker")
    try:
        topicos = AdminClient({"bootstrap.servers": v.endereco, **SILENCIO}).list_topics(timeout=15).topics
    except KafkaException as erro:
        raise Falhou(f"o broker não respondeu: {erro}")
    faltam = [t for t in TOPICOS_S3 if t not in topicos]
    if faltam:
        raise Falhou(f"faltam {len(faltam)} tópico(s): {', '.join(faltam)}. Pedir à TRA-1 para criar")
    ok(f"os {len(TOPICOS_S3)} tópicos existem")

    v.assinar()

    print("\n[2] Criar um alerta por comando (A5 → S3)")
    criar = v.publicar(comando("ods.s3.alert.command", {
        "title": "[TESTE S3] verificação do broker",
        "message": "Alerta de teste do S3; é encerrado logo em seguida",
        "severity": "LOW",
        "recipients": ["role:supervisor"],
        "channels": ["SCREEN"],
    }))
    payload = v.resposta("ods.s3.alert.reply", criar)
    if payload.get("status") != "OK":
        raise Falhou(f"o S3 recusou a criação: {payload.get('error')}")
    alerta_id = payload["alert_id"]
    ok(f"S3 respondeu OK: alert_id {alerta_id}")

    alerta = v.esperar(
        "ods.s3.alert", lambda a: a["payload"].get("alert_id") == alerta_id, "o alerta criado",
    )
    ok(f"S3 publicou o alerta em ods.s3.alert (state {alerta['payload']['state']}, sequence {alerta['sequence']})")

    print("\n[3] Comandos sobre o alerta")
    acao(v, alerta_id, "ACKNOWLEDGE", "ACKNOWLEDGED")
    acao(v, alerta_id, "CLOSE", "CLOSED")
    v.esperar(
        "ods.s3.alert",
        lambda a: a["payload"].get("alert_id") == alerta_id and a["payload"]["state"] == "CLOSED",
        "a republicação CLOSED",
    )
    ok("S3 republicou o alerta a cada mudança, até CLOSED")

    print("\n[4] Consulta ao histórico")
    consulta = v.publicar(comando("ods.s3.alerts.query", {"severity": "LOW", "state": "CLOSED", "limit": 100}))
    payload = v.resposta("ods.s3.alerts.reply", consulta)
    if not any(a.get("alert_id") == alerta_id for a in payload.get("alerts", [])):
        raise Falhou("o alerta de teste não veio na consulta ao histórico")
    ok("o alerta de teste está no histórico, CLOSED")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--broker", help="host:porta (padrão: S3_KAFKA_BOOTSTRAP)")
    parser.add_argument("--broker-oficial", action="store_true",
                        help="confirma que é para usar um broker fora de localhost")
    parser.add_argument("--prazo", type=float, default=60, help="segundos até desistir (padrão 60)")
    argumentos = parser.parse_args()

    endereco = argumentos.broker or os.environ.get("S3_KAFKA_BOOTSTRAP")
    if not endereco:
        parser.error("informe --broker ou defina S3_KAFKA_BOOTSTRAP")
    locais = all(item.strip().rsplit(":", 1)[0] in ("localhost", "127.0.0.1") for item in endereco.split(","))
    if not locais and not argumentos.broker_oficial:
        parser.error(
            f"{endereco} não é local. O broker do projeto é compartilhado por todos os squads: "
            "confirme que é o broker certo e rode com --broker-oficial."
        )

    print(f"Verificação do S3 no broker {endereco} (até {argumentos.prazo:.0f} s)")
    v = Verificador(endereco, argumentos.prazo)
    try:
        verificar(v)
    except Falhou as erro:
        print(f"\nFALHOU: {erro}")
        return 1
    finally:
        v.fechar()

    print(f"\nTudo certo: o S3 recebe e publica neste broker. "
          f"Mensagens enviadas: {v.enviadas}; recebidas: {len(v.recebidas)}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
