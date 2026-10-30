"""
Exemplo real de ods.s4.clip mandado pelo S4 (MOD-2) em 29/10/2026,
guardado sem alteração em dados/clipe_s4_real.json.

Hoje o S3 o rejeita por quatro motivos, cada um suficiente sozinho:
falta captured_at, falta sequence (perfil EV-SERVIÇO, seção 3),
session_id e clip_id não são UUID (seção 2, regra 3). Com as quatro
correções, o mesmo clipe é aceito e vinculado ao alerta.
"""

import copy
import json
import unittest

import contrato
from s3.errors import INVALID_MESSAGE, INVALID_PAYLOAD, ErroS3
from s3.ingestion import IngestorClipes, IngestorEventos
from s3.service import AlertService

from fabricas import PublicadorFalso, ler_json, mensagem_evento, roteador

REAL = ler_json("clipe_s4_real.json")
EVENT_ID = REAL["payload"]["event_id"]
CLIP_ID_SEM_PREFIXO = REAL["payload"]["clip_id"].removeprefix("clip-")


def corrigido() -> dict:
    """O exemplo real com só as quatro correções pedidas ao S4."""

    dados = copy.deepcopy(REAL)
    dados["captured_at"] = dados["payload"]["start_at"]
    dados["sequence"] = 0
    dados["session_id"] = "9c2a1f3e-4d5b-4c6a-9e8f-0a1b2c3d4e5f"
    dados["payload"]["clip_id"] = CLIP_ID_SEM_PREFIXO
    return dados


# Cada problema, aplicado sozinho sobre a versão corrigida.
PROBLEMAS = {
    "falta captured_at": (lambda d: d.pop("captured_at"), INVALID_MESSAGE),
    "falta sequence": (lambda d: d.pop("sequence"), INVALID_MESSAGE),
    "session_id exec-demo-1": (
        lambda d: d.update(session_id=REAL["session_id"]), INVALID_MESSAGE
    ),
    "clip_id com prefixo clip-": (
        lambda d: d["payload"].update(clip_id=REAL["payload"]["clip_id"]),
        INVALID_PAYLOAD,
    ),
}


class TestExemploRealDoS4(unittest.TestCase):

    def setUp(self):
        self.publicador = PublicadorFalso()
        self.service = AlertService(
            roteador=roteador(captura=True), publicador=self.publicador
        )
        self.clipes = IngestorClipes(self.service)
        IngestorEventos(self.service).receber(
            json.dumps(mensagem_evento({"event_id": EVENT_ID}))
        )
        self.alerta = self.service.repository.buscar_por_evento(EVENT_ID)
        self.publicadas = len(self.publicador.mensagens)

    def test_exemplo_real_rejeitado(self):
        with self.assertRaises(ErroS3) as contexto:
            self.clipes.receber(json.dumps(REAL))

        self.assertEqual(contexto.exception.codigo, INVALID_MESSAGE)
        self.assertIsNone(self.alerta.clip_id)
        self.assertEqual(len(self.publicador.mensagens), self.publicadas)

    def test_cada_problema_rejeita_sozinho(self):
        for nome, (estragar, codigo) in PROBLEMAS.items():
            with self.subTest(nome):
                dados = corrigido()
                estragar(dados)

                with self.assertRaises(ErroS3) as contexto:
                    self.clipes.receber(json.dumps(dados))

                self.assertEqual(contexto.exception.codigo, codigo)
                self.assertIsNone(self.alerta.clip_id)

    def test_corrigido_e_aceito_e_vinculado(self):
        dados = corrigido()
        contrato.envelope(dados, "EV-SERVIÇO")

        alerta = self.clipes.receber(json.dumps(dados))

        self.assertEqual(alerta.clip_id, CLIP_ID_SEM_PREFIXO)
        republicado = self.publicador.mensagens[-1]
        contrato.mensagem_alerta(republicado)
        self.assertEqual(republicado["payload"]["clip_id"], CLIP_ID_SEM_PREFIXO)
        self.assertEqual(republicado["payload"]["state"], "CREATED")

    def test_resto_do_exemplo_ja_esta_conforme(self):
        """media_uri s4://, points_of_interest vazio e horários iguais passam."""

        dados = corrigido()
        self.assertEqual(dados["payload"]["points_of_interest"], [])
        self.assertTrue(dados["payload"]["media_uri"].startswith("s4://clips/"))

        self.assertIsNotNone(self.clipes.receber(json.dumps(dados)))

    def test_captura_manual_com_event_id_null(self):
        """Captura manual: o S4 manda "event_id": null; aceito e ignorado."""

        dados = corrigido()
        dados["payload"]["event_id"] = None

        self.assertIsNone(self.clipes.receber(json.dumps(dados)))
        self.assertIsNone(self.alerta.clip_id)
        self.assertEqual(len(self.publicador.mensagens), self.publicadas)


if __name__ == "__main__":
    unittest.main()
