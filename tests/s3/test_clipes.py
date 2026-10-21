"""
ods.s4.clip → clip_id no alerta (capture: true no ROUTING).
Decisão de 21/10/2026: o S3 recebe o descritor do S4.
"""

import json
import unittest
from unittest import mock
from uuid import uuid4

import contrato
from s3 import repository as modulo_repositorio
from s3.contracts import CAMPOS_CLIPE, DescritorClipe
from s3.errors import INVALID_MESSAGE, INVALID_PAYLOAD, ErroS3
from s3.ingestion import IngestorClipes, IngestorEventos
from s3.kafka_adapter import TOPICOS_ENTRADA
from s3.service import AlertService
from s3.subscription import ConectorComandos, ConectorEventos, DespachanteS3
from s3.commands import ProcessadorComandos

from fabricas import (
    AssinanteFalso,
    PublicadorFalso,
    comando,
    ler_json,
    mensagem_evento,
    roteador,
)


def mensagem_clipe(payload=None, **envelope):
    """ods.s4.clip do exemplo oficial, com message_id novo."""

    dados = ler_json("clipe.json")
    dados["message_id"] = str(uuid4())
    dados["payload"].update(payload or {})
    dados.update(envelope)
    return dados


class BaseClipes(unittest.TestCase):

    captura = True

    def setUp(self):
        self.publicador = PublicadorFalso()
        self.service = AlertService(
            roteador=roteador(captura=self.captura), publicador=self.publicador
        )
        self.eventos = IngestorEventos(self.service)
        self.clipes = IngestorClipes(self.service)
        self.evento = mensagem_evento()
        self.event_id = self.evento["payload"]["event_id"]

    def receber_evento(self):
        return self.eventos.receber(json.dumps(self.evento))

    def receber_clipe(self, **alteracoes):
        dados = mensagem_clipe({"event_id": self.event_id, **alteracoes})
        self.clipes.receber(json.dumps(dados))
        return dados

    def estados(self):
        return [(m["payload"]["state"], m["payload"].get("clip_id")) for m in self.publicador.mensagens]


class TestDescritorClipe(unittest.TestCase):

    def test_exemplo_oficial(self):
        dados = ler_json("clipe.json")

        contrato.envelope(dados, "EV-SERVIÇO")
        clipe = DescritorClipe.de_mensagem(dados)

        self.assertEqual(clipe.clip_id, dados["payload"]["clip_id"])
        self.assertEqual(clipe.evento_id, dados["payload"]["event_id"])
        self.assertEqual(clipe.tipo_midia, "CLIP")

    def test_campos_obrigatorios(self):
        for campo in CAMPOS_CLIPE:
            with self.subTest(campo=campo):
                dados = mensagem_clipe()
                del dados["payload"][campo]

                with self.assertRaises(ErroS3) as contexto:
                    DescritorClipe.de_mensagem(dados)

                self.assertEqual(contexto.exception.codigo, INVALID_PAYLOAD)

    def test_valores_invalidos(self):
        for alteracao in (
            {"clip_id": "clip-1"},
            {"event_id": "evt-1"},
            {"media_type": "VIDEO"},
            {"expires_at": "amanhã"},
        ):
            with self.subTest(alteracao=alteracao):
                with self.assertRaises(ErroS3):
                    DescritorClipe.de_mensagem(mensagem_clipe(alteracao))


class TestVinculoDoClipe(BaseClipes):

    def test_clipe_depois_do_alerta(self):
        """O alerta é republicado com o clip_id e o mesmo state."""

        self.receber_evento()
        clipe = self.receber_clipe()
        clip_id = clipe["payload"]["clip_id"]

        self.assertEqual(self.estados(), [("CREATED", None), ("CREATED", clip_id)])

        segunda = self.publicador.mensagens[1]
        contrato.mensagem_alerta(segunda)
        self.assertEqual(segunda["sequence"], 1)
        self.assertNotEqual(segunda["message_id"], self.publicador.mensagens[0]["message_id"])

        alerta = self.service.repository.buscar_por_evento(self.event_id)
        self.assertEqual(alerta.clip_id, clip_id)

    def test_clipe_antes_do_alerta(self):
        """O clipe fica guardado e entra na criação: uma publicação só."""

        clipe = self.receber_clipe()
        self.assertEqual(self.publicador.mensagens, [])

        self.receber_evento()

        self.assertEqual(self.estados(), [("CREATED", clipe["payload"]["clip_id"])])

    def test_vale_o_primeiro_clipe(self):
        self.receber_evento()
        primeiro = self.receber_clipe()
        self.receber_clipe(clip_id=str(uuid4()))

        self.assertEqual(len(self.publicador.mensagens), 2)
        self.assertEqual(
            self.service.repository.buscar_por_evento(self.event_id).clip_id,
            primeiro["payload"]["clip_id"],
        )

    def test_clipe_sem_evento_ignorado(self):
        self.receber_evento()
        dados = mensagem_clipe()
        del dados["payload"]["event_id"]

        self.clipes.receber(json.dumps(dados))

        self.assertEqual(len(self.publicador.mensagens), 1)

    def test_clipe_repetido_pelo_message_id(self):
        self.receber_evento()
        dados = mensagem_clipe({"event_id": self.event_id})

        self.clipes.receber(json.dumps(dados))
        self.clipes.receber(json.dumps(dados))

        self.assertEqual(len(self.publicador.mensagens), 2)

    def test_clipe_depois_do_encerramento(self):
        """A evidência é vinculada mesmo com o alerta já encerrado."""

        alerta = self.receber_evento()
        self.service.encerrar_alerta(alerta.alerta_id, "user:maria")
        clipe = self.receber_clipe()

        self.assertEqual(self.estados()[-1], ("CLOSED", clipe["payload"]["clip_id"]))

    def test_historico_nao_registra_o_clipe(self):
        """O vínculo não é ação humana: o histórico só tem a criação."""

        alerta = self.receber_evento()
        self.receber_clipe()

        historico = self.service.repository.buscar_historico(alerta.alerta_id)
        self.assertEqual([r.acao for r in historico], ["CREATED"])

    def test_consulta_traz_o_clip_id(self):
        self.receber_evento()
        clipe = self.receber_clipe()

        resposta = json.loads(ProcessadorComandos(self.service).receber(
            json.dumps(comando("ods.s3.alerts.query", {}))
        ))

        self.assertEqual(
            resposta["payload"]["alerts"][0]["clip_id"], clipe["payload"]["clip_id"]
        )

    def test_limite_de_clipes_pendentes(self):
        """Clipes de eventos sem alerta não acumulam sem limite."""

        with mock.patch.object(modulo_repositorio, "LIMITE_CLIPES_PENDENTES", 2):
            eventos = [str(uuid4()) for _ in range(3)]

            for event_id in eventos:
                self.clipes.receber(json.dumps(mensagem_clipe({"event_id": event_id})))

            repositorio = self.service.repository
            self.assertIsNone(repositorio.retirar_clipe_pendente(eventos[0]))
            self.assertIsNotNone(repositorio.retirar_clipe_pendente(eventos[2]))


class TestSemCaptura(BaseClipes):
    """Rota com capture: false não recebe clipe."""

    captura = False

    def test_clipe_ignorado(self):
        self.receber_evento()
        self.receber_clipe()

        self.assertEqual(self.estados(), [("CREATED", None)])

    def test_clipe_antes_do_alerta_ignorado(self):
        self.receber_clipe()
        self.receber_evento()

        self.assertEqual(self.estados(), [("CREATED", None)])


class TestEntradaDoClipe(BaseClipes):

    def test_so_o_s4_produz(self):
        with self.assertRaises(ErroS3) as contexto:
            self.clipes.receber(json.dumps(mensagem_clipe(producer="S2")))

        self.assertEqual(contexto.exception.codigo, INVALID_MESSAGE)

    def test_envelope_invalido(self):
        for alteracao in ({"session_id": None}, {"schema": "ods.s2.event"}):
            with self.subTest(alteracao=alteracao):
                with self.assertRaises(ErroS3):
                    self.clipes.receber(json.dumps(mensagem_clipe(**alteracao)))

    def test_despachante_encaminha_o_clipe(self):
        assinante = AssinanteFalso()
        DespachanteS3(
            ConectorEventos(assinante, self.eventos),
            ConectorComandos(assinante, ProcessadorComandos(self.service), PublicadorFalso()),
            ConectorEventos(assinante, self.clipes),
        ).conectar(assinante)

        assinante.publicar(json.dumps(self.evento))
        assinante.publicar(json.dumps(mensagem_clipe({"event_id": self.event_id})))

        self.assertEqual(len(self.publicador.mensagens), 2)

    def test_despachante_sem_conector_de_clipes(self):
        assinante = AssinanteFalso()
        DespachanteS3(
            ConectorEventos(assinante, self.eventos),
            ConectorComandos(assinante, ProcessadorComandos(self.service), PublicadorFalso()),
        ).conectar(assinante)

        with self.assertLogs("s3.subscription", level="WARNING"):
            self.assertIsNone(assinante.publicar(json.dumps(mensagem_clipe())))

    def test_topico_assinado(self):
        self.assertIn("ods.s4.clip", TOPICOS_ENTRADA)


if __name__ == "__main__":
    unittest.main()
