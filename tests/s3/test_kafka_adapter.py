"""
Testes do adaptador Kafka com cliente falso.

Nenhum teste acessa o broker real: ele é pago e
compartilhado (Guia do Kafka, 4.5).
"""

import json
import os
import unittest
from unittest import mock

from s3.kafka_adapter import (
    TOPICOS_COMANDOS,
    TOPICOS_ENTRADA,
    VARIAVEL_BROKER,
    AssinanteKafka,
    PublicadorKafka,
    endereco_broker,
    topicos_entrada,
)


class MensagemFalsa:
    def __init__(self, valor, erro=None):
        self._valor = valor
        self._erro = erro

    def value(self):
        return self._valor

    def error(self):
        return self._erro


class ConsumidorFalso:
    def __init__(self, mensagens):
        self.mensagens = list(mensagens)
        self.topicos = None
        self.commits = []
        self.fechado = False

    def subscribe(self, topicos):
        self.topicos = topicos

    def poll(self, intervalo):
        return self.mensagens.pop(0) if self.mensagens else None

    def commit(self, message, asynchronous):
        self.commits.append(message)

    def close(self):
        self.fechado = True


class ProdutorFalso:
    """Simula o confluent-kafka: o relatório de entrega sai no flush."""

    def __init__(self, pendentes=0, erro_entrega=None):
        self.enviadas = []
        self.chaves = []
        self.pendentes = pendentes
        self.erro_entrega = erro_entrega
        self._relatorios = []

    def produce(self, topic, value, key=None, on_delivery=None):
        self.enviadas.append((topic, value))
        self.chaves.append(key)
        self._relatorios.append(on_delivery)

    def flush(self, tempo):
        for relatorio in self._relatorios:
            if relatorio is not None:
                relatorio(self.erro_entrega, None)

        self._relatorios = []
        return self.pendentes


class TestEnderecoBroker(unittest.TestCase):

    def test_sem_endereco(self):
        """O endereço não fica fixo no código."""

        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                endereco_broker()

    def test_variavel_de_ambiente(self):
        with mock.patch.dict(os.environ, {VARIAVEL_BROKER: "broker:9093"}):
            self.assertEqual(endereco_broker(), "broker:9093")


class TestAssinanteKafka(unittest.TestCase):

    def test_entrega_e_commit(self):
        consumidor = ConsumidorFalso([
            MensagemFalsa(b"a"),
            MensagemFalsa(b"b"),
        ])
        recebidas = []

        assinante = AssinanteKafka(consumidor=consumidor)
        assinante.assinar(recebidas.append)

        entregues = assinante.executar(max_mensagens=2, intervalo_poll_s=0)

        self.assertEqual(entregues, 2)
        self.assertEqual(recebidas, [b"a", b"b"])
        self.assertEqual(len(consumidor.commits), 2)
        self.assertIn("ods.s2.event", consumidor.topicos)
        self.assertTrue(consumidor.fechado)

    def test_para_pelo_tempo_limite(self):
        """Sem mensagens, o laço termina pelo tempo limite."""

        consumidor = ConsumidorFalso([])
        assinante = AssinanteKafka(consumidor=consumidor)
        assinante.assinar(lambda mensagem: None)

        entregues = assinante.executar(tempo_limite_s=0.05, intervalo_poll_s=0)

        self.assertEqual(entregues, 0)
        self.assertTrue(consumidor.fechado)

    def test_falha_no_callback_sem_commit(self):
        """Se o processamento falhar, o offset não avança."""

        consumidor = ConsumidorFalso([MensagemFalsa(b"a")])

        def falhar(mensagem):
            raise ConnectionError("barramento indisponível")

        assinante = AssinanteKafka(consumidor=consumidor)
        assinante.assinar(falhar)

        with self.assertLogs("s3.kafka_adapter", level="ERROR"):
            assinante.executar(tempo_limite_s=0.05, intervalo_poll_s=0)

        self.assertEqual(consumidor.commits, [])

    def test_erro_de_leitura_ignorado(self):
        consumidor = ConsumidorFalso([MensagemFalsa(None, erro="falha")])
        assinante = AssinanteKafka(consumidor=consumidor)
        assinante.assinar(lambda mensagem: None)

        with self.assertLogs("s3.kafka_adapter", level="WARNING"):
            entregues = assinante.executar(tempo_limite_s=0.05, intervalo_poll_s=0)

        self.assertEqual(entregues, 0)

    def test_executar_sem_assinar(self):
        with self.assertRaises(RuntimeError):
            AssinanteKafka(consumidor=ConsumidorFalso([])).executar(max_mensagens=1)


class TestTopicosDeEntrada(unittest.TestCase):

    def test_com_roteamento_assina_tudo(self):
        self.assertEqual(topicos_entrada(True), TOPICOS_ENTRADA)
        self.assertIn("ods.s2.event", TOPICOS_ENTRADA)

    def test_sem_roteamento_so_comandos(self):
        """Build sem S2 (A7): não assina ods.s2.event nem ods.s4.clip."""

        topicos = topicos_entrada(False)

        self.assertEqual(topicos, TOPICOS_COMANDOS)
        self.assertNotIn("ods.s2.event", topicos)
        self.assertNotIn("ods.s4.clip", topicos)


class TestPublicadorKafka(unittest.TestCase):

    def test_topico_pelo_schema(self):
        """Um tópico por schema."""

        produtor = ProdutorFalso()
        publicador = PublicadorKafka(produtor=produtor)

        publicador.publicar(json.dumps({"schema": "ods.s3.alert"}))

        topico, valor = produtor.enviadas[0]

        self.assertEqual(topico, "ods.s3.alert")
        self.assertIsInstance(valor, bytes)

    def test_mapa_de_topicos(self):
        """Permite seguir outra nomenclatura sem mudar o código."""

        produtor = ProdutorFalso()
        publicador = PublicadorKafka(
            produtor=produtor,
            topicos={"ods.s3.alert": "ods.alerta.emitido"},
        )

        publicador.publicar(json.dumps({"schema": "ods.s3.alert"}))

        self.assertEqual(produtor.enviadas[0][0], "ods.alerta.emitido")

    def test_chave_de_particao(self):
        """
        ods.s3.alert usa o alert_id como chave, para manter a ordem
        das versões do mesmo alerta; replies vão sem chave.
        """

        produtor = ProdutorFalso()
        publicador = PublicadorKafka(produtor=produtor)
        alerta_id = "0f1e2d3c-4b5a-4968-8776-655443322110"

        publicador.publicar(json.dumps(
            {"schema": "ods.s3.alert", "payload": {"alert_id": alerta_id}}
        ))
        publicador.publicar(json.dumps(
            {"schema": "ods.s3.alert.reply", "payload": {"status": "OK"}}
        ))

        self.assertEqual(produtor.chaves, [alerta_id.encode("utf-8"), None])

    def test_broker_recusa_a_mensagem(self):
        """
        flush() devolve 0, mas o relatório de entrega traz erro
        (por exemplo, tópico inexistente): é falha, não sucesso.
        """

        publicador = PublicadorKafka(
            produtor=ProdutorFalso(erro_entrega="UNKNOWN_TOPIC_OR_PART")
        )

        with self.assertRaises(ConnectionError) as contexto:
            publicador.publicar(json.dumps({"schema": "ods.s3.alert"}))

        self.assertIn("UNKNOWN_TOPIC_OR_PART", str(contexto.exception))

    def test_nao_entregue(self):
        publicador = PublicadorKafka(produtor=ProdutorFalso(pendentes=1))

        with self.assertRaises(ConnectionError):
            publicador.publicar(json.dumps({"schema": "ods.s3.alert"}))


if __name__ == "__main__":
    unittest.main()
