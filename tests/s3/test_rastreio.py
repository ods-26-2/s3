"""
Rastreabilidade (Sprint 3, S3.2): uma linha de log por mensagem
que entra ou sai, com os identificadores que ligam uma à outra.
"""

import json
import unittest

from s3.commands import ProcessadorComandos
from s3.ingestion import IngestorEventos
from s3.rastreio import linha
from s3.service import AlertService
from s3.subscription import ConectorComandos, ConectorEventos, DespachanteS3

from fabricas import AssinanteFalso, PublicadorFalso, ler_json, mensagem_evento, roteador


class TestLinha(unittest.TestCase):

    def test_formato_chave_valor(self):
        texto = linha(
            "entrada",
            {"schema": "ods.s2.event", "message_id": "m-1", "producer": "S2", "sequence": 0},
            resultado="alerta_criado",
            alert_id=None,
        )

        self.assertEqual(
            texto, "entrada schema=ods.s2.event message_id=m-1 producer=S2 sequence=0 resultado=alerta_criado"
        )

    def test_valor_com_espaco_vai_entre_aspas(self):
        self.assertIn('motivo="campo ausente"', linha("descartada", None, motivo="campo ausente"))


class TestRegistros(unittest.TestCase):

    def setUp(self):
        self.service = AlertService(roteador=roteador(), publicador=PublicadorFalso())
        self.assinante = AssinanteFalso()
        DespachanteS3(
            ConectorEventos(self.assinante, IngestorEventos(self.service)),
            ConectorComandos(self.assinante, ProcessadorComandos(self.service), PublicadorFalso()),
        ).conectar(self.assinante)

    def registros(self, mensagem):
        with self.assertLogs("s3", level="INFO") as captura:
            self.assinante.publicar(json.dumps(mensagem) if isinstance(mensagem, dict) else mensagem)

        return captura.output

    def test_evento_alerta_e_saida_ligados(self):
        evento = mensagem_evento()
        saida = self.registros(evento)

        # assertLogs devolve "NÍVEL:logger:mensagem".
        entrada = next(l for l in saida if ":entrada " in l)
        publicada = next(l for l in saida if "saida schema=ods.s3.alert " in l)
        alert_id = self.service.repository.buscar_por_evento(evento["payload"]["event_id"]).alerta_id

        self.assertIn(f"message_id={evento['message_id']}", entrada)
        self.assertIn("resultado=alerta_criado", entrada)
        self.assertIn(f"alert_id={alert_id}", entrada)
        self.assertIn(f"correlation_id={evento['payload']['event_id']}", publicada)
        self.assertIn(f"alert_id={alert_id}", publicada)
        self.assertIn("state=CREATED", publicada)

    def test_repetida(self):
        evento = mensagem_evento()
        self.registros(evento)

        self.assertTrue(any("resultado=repetida" in l for l in self.registros(evento)))

    def test_comando_e_resposta_ligados(self):
        evento = mensagem_evento()
        self.registros(evento)
        acao = ler_json("comando_acao.json")
        acao["payload"]["alert_id"] = self.service.repository.buscar_por_evento(
            evento["payload"]["event_id"]
        ).alerta_id

        saida = self.registros(acao)

        self.assertTrue(any("action=MUTE resultado=OK" in l and "state=MUTED" in l for l in saida))
        self.assertTrue(any(
            "saida schema=ods.s3.alert_action.reply" in l and f"correlation_id={acao['message_id']}" in l
            for l in saida
        ))

    def test_erro_de_regra_registra_o_codigo(self):
        acao = ler_json("comando_acao.json")
        acao["payload"]["alert_id"] = "0f1e2d3c-4b5a-4968-8776-655443322110"

        saida = self.registros(acao)

        self.assertTrue(any("resultado=ERROR code=ALERT_NOT_FOUND" in l for l in saida))

    def test_descartada_com_motivo(self):
        with self.assertLogs("s3.subscription", level="WARNING") as captura:
            self.assinante.publicar('{"schema": "ods.s2.event", "message_id": "x"}')

        self.assertIn("descartada schema=ods.s2.event message_id=x motivo=", captura.output[0])


if __name__ == "__main__":
    unittest.main()
