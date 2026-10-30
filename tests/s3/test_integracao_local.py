"""
Teste integrado local S2 → S3 → A5 (preparação para o teste real, Sprint 3).

Roda o próprio `python -m s3` (src/s3/__main__.py), com o adaptador Kafka,
o SQLite, o ROUTING e o arquivo de permissões. Só o broker é trocado por um
broker em memória que imita o Kafka: tópicos, chave de partição, offset
confirmado por grupo de consumidores e relatório de entrega (on_delivery).
Cada execução do main é uma partida do S3, então o reinício também é testado.

Não é a integração com a aplicação real: o S2 e o A5 são simulados aqui.
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from collections import defaultdict
from unittest import mock
from uuid import uuid4

import contrato
import s3.__main__ as principal
from s3.kafka_adapter import AssinanteKafka, PublicadorKafka

from fabricas import comando, mensagem_evento

DADOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dados")
ACAO = "ods.s3.alert_action.command"


# ----------------------------------------------------------------
# Broker em memória com a interface do confluent-kafka usada pelo S3


class MensagemKafka:
    def __init__(self, topico, chave, valor, offset):
        self._topico, self._chave, self._valor, self.offset = topico, chave, valor, offset

    def topic(self):
        return self._topico

    def key(self):
        return self._chave

    def value(self):
        return self._valor

    def error(self):
        return None


class BrokerMemoria:
    def __init__(self):
        self.topicos = defaultdict(list)
        self.confirmados = {}  # (grupo, tópico) → próximo offset a ler

    def produtor(self):
        return ProdutorMemoria(self)

    def consumidor(self, grupo):
        return ConsumidorMemoria(self, grupo)

    def lidas(self, topico):
        return [json.loads(m.value()) for m in self.topicos[topico]]


class ProdutorMemoria:
    def __init__(self, broker):
        self.broker = broker
        self.fila = []
        self.recusar = None  # erro que o broker devolve no relatório de entrega

    def produce(self, topic, value, key=None, on_delivery=None):
        self.fila.append((topic, value, key, on_delivery))

    def flush(self, tempo=None):
        for topico, valor, chave, relatorio in self.fila:
            if self.recusar:
                if relatorio:
                    relatorio(self.recusar, None)
                continue

            mensagens = self.broker.topicos[topico]
            mensagem = MensagemKafka(topico, chave, valor, len(mensagens))
            mensagens.append(mensagem)

            if relatorio:
                relatorio(None, mensagem)

        self.fila = []
        return 0


class ConsumidorMemoria:
    """A posição avança no poll; só o commit fica para a próxima partida."""

    def __init__(self, broker, grupo):
        self.broker, self.grupo = broker, grupo
        self.posicao = {}

    def subscribe(self, topicos):
        self.posicao = {t: self.broker.confirmados.get((self.grupo, t), 0) for t in topicos}

    def poll(self, intervalo):
        for topico, posicao in self.posicao.items():
            mensagens = self.broker.topicos[topico]
            if posicao < len(mensagens):
                self.posicao[topico] = posicao + 1
                return mensagens[posicao]
        return None

    def commit(self, message, asynchronous=False):
        self.broker.confirmados[(self.grupo, message.topic())] = message.offset + 1

    def close(self):
        pass


# ----------------------------------------------------------------


class TestFluxoS2S3A5(unittest.TestCase):

    def setUp(self):
        self.pasta = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.pasta, ignore_errors=True)
        self.banco = os.path.join(self.pasta, "s3.sqlite3")

        self.broker = BrokerMemoria()
        self.produtor_s3 = self.broker.produtor()
        self.s2 = PublicadorKafka(produtor=self.broker.produtor())
        self.a5 = PublicadorKafka(produtor=self.broker.produtor())

    def partida_s3(self, mensagens):
        """Uma execução do python -m s3, até processar `mensagens` mensagens."""

        argv = [
            "s3",
            "--routing", os.path.join(DADOS, "routing_a5.json"),
            "--versao-routing", "routing-a5-1",
            "--permissoes", os.path.join(DADOS, "permissoes_a5.json"),
            "--banco", self.banco,
            "--max-mensagens", str(mensagens),
            "--tempo-limite", "5",
        ]

        def assinante(topicos):
            return AssinanteKafka(topicos=topicos, consumidor=self.broker.consumidor("S3-alertas"))

        with mock.patch.object(principal, "AssinanteKafka", assinante), \
                mock.patch.object(principal, "PublicadorKafka", lambda: PublicadorKafka(produtor=self.produtor_s3)), \
                mock.patch.object(sys, "argv", argv):
            principal.main()

    def alertas(self):
        return self.broker.lidas("ods.s3.alert")

    def respostas(self, topico):
        return self.broker.lidas(topico)

    def acao(self, alerta_id, action, actor, **extra):
        dados = comando(ACAO, {"alert_id": alerta_id, "action": action, "actor": actor, **extra})
        self.a5.publicar(json.dumps(dados))
        return dados

    def test_fluxo_completo(self):
        # 1–5. O S2 publica o evento; o S3 cria, grava e publica o alerta.
        evento = mensagem_evento()
        self.s2.publicar(json.dumps(evento))
        self.partida_s3(1)

        [criado] = self.alertas()
        contrato.mensagem_alerta(criado)
        alerta_id = criado["payload"]["alert_id"]

        # 6. O A5 recebe o alerta, com o contexto do evento e a chave alert_id.
        self.assertEqual(criado["payload"]["state"], "CREATED")
        self.assertEqual(criado["correlation_id"], evento["payload"]["event_id"])
        self.assertEqual(criado["session_id"], evento["session_id"])
        self.assertEqual(criado["artifact_version"], "routing-a5-1")
        self.assertEqual(criado["payload"]["title"], "EPI ausente: luva")
        self.assertEqual(self.broker.topicos["ods.s3.alert"][0].key(), alerta_id.encode("utf-8"))

        # 7–10. ACK, uma tentativa sem permissão, MUTE e CLOSE, na ordem permitida.
        ack = self.acao(alerta_id, "ACKNOWLEDGE", "user:maria", reason="a caminho")
        negado = self.acao(alerta_id, "MUTE", "user:ana", mute_until="2030-01-01T00:00:00.000Z")
        mute = self.acao(alerta_id, "MUTE", "user:maria", mute_until="2030-01-01T00:00:00.000Z")
        close = self.acao(alerta_id, "CLOSE", "user:maria", reason="EPI colocado")
        self.partida_s3(4)

        # 8. As respostas chegam ligadas aos pedidos pelo correlation_id.
        respostas = {r["correlation_id"]: r for r in self.respostas("ods.s3.alert_action.reply")}
        for resposta in respostas.values():
            contrato.envelope(resposta, "RPL")

        self.assertEqual(respostas[ack["message_id"]]["payload"]["state"], "ACKNOWLEDGED")
        self.assertEqual(respostas[negado["message_id"]]["payload"]["error"]["code"], "UNAUTHORIZED_ACTOR")
        self.assertEqual(respostas[mute["message_id"]]["payload"]["state"], "MUTED")
        self.assertEqual(respostas[close["message_id"]]["payload"]["state"], "CLOSED")

        # 9. Cada mudança foi republicada, na ordem, com sequence crescente.
        estados = [a["payload"]["state"] for a in self.alertas()]
        self.assertEqual(estados, ["CREATED", "ACKNOWLEDGED", "MUTED", "CLOSED"])
        self.assertEqual([a["sequence"] for a in self.alertas()], [0, 1, 2, 3])
        self.assertEqual(len({a["message_id"] for a in self.alertas()}), 4)

        # 11–12. A consulta ao histórico devolve o estado gravado.
        consulta = comando("ods.s3.alerts.query", {"severity": "HIGH"})
        self.a5.publicar(json.dumps(consulta))
        self.partida_s3(1)

        [historico] = self.respostas("ods.s3.alerts.reply")
        contrato.envelope(historico, "RPL")
        [item] = historico["payload"]["alerts"]
        self.assertEqual(item["state"], "CLOSED")
        self.assertEqual(item["acknowledged_by"], "user:maria")
        self.assertEqual(item["muted_until"], "2030-01-01T00:00:00.000Z")
        self.assertEqual(item["closed_by"], "user:maria")

        # 13–14. Nova partida: o S2 reentrega o mesmo evento. Nada duplica,
        # e o histórico continua lá.
        self.s2.publicar(json.dumps(evento))
        consulta2 = comando("ods.s3.alerts.query", {})
        self.a5.publicar(json.dumps(consulta2))
        self.partida_s3(2)

        self.assertEqual(len(self.alertas()), 4)
        ultima = self.respostas("ods.s3.alerts.reply")[-1]
        self.assertEqual(ultima["correlation_id"], consulta2["message_id"])
        self.assertEqual([a["alert_id"] for a in ultima["payload"]["alerts"]], [alerta_id])

    def test_queda_do_broker_e_recuperacao(self):
        """15. O broker recusa a publicação; o alerta sai na partida seguinte, uma vez só."""

        self.produtor_s3.recusar = "BROKER_INDISPONIVEL"
        evento = mensagem_evento({"event_id": str(uuid4())})
        self.s2.publicar(json.dumps(evento))

        with self.assertLogs("s3.service", level="ERROR"):
            self.partida_s3(1)

        self.assertEqual(self.alertas(), [])

        # O broker volta: a fila de saída é reenviada na partida.
        self.produtor_s3.recusar = None
        self.partida_s3(0)

        [alerta] = self.alertas()
        contrato.mensagem_alerta(alerta)
        self.assertEqual(alerta["correlation_id"], evento["payload"]["event_id"])

        # Outra partida não publica de novo.
        self.partida_s3(0)
        self.assertEqual(len(self.alertas()), 1)

    def test_comando_repetido_depois_do_reinicio(self):
        """O mesmo comando, entregue de novo depois de uma partida, recebe a mesma resposta."""

        with open(os.path.join(DADOS, "comando_criar.json"), encoding="utf-8") as arquivo:
            criar = json.load(arquivo)
        self.a5.publicar(json.dumps(criar))
        self.partida_s3(1)

        self.a5.publicar(json.dumps(criar))
        self.partida_s3(1)

        primeira, segunda = self.respostas("ods.s3.alert.reply")
        self.assertEqual(primeira["payload"], segunda["payload"])
        self.assertEqual(len(self.alertas()), 1)
        self.assertNotIn("session_id", self.alertas()[0])


if __name__ == "__main__":
    unittest.main()
