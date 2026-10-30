
import json
import unittest
from uuid import uuid4

from s3.commands import ProcessadorComandos
from s3.errors import ErroS3
from s3.service import AlertService

from fabricas import (
    PublicadorFalso,
    comando,
    evento,
    ler_json,
    roteador,
)

ACAO = "ods.s3.alert_action.command"
CRIAR = "ods.s3.alert.command"
CONSULTA = "ods.s3.alerts.query"


class BaseComandos(unittest.TestCase):

    def setUp(self):
        self.publicador = PublicadorFalso()
        self.service = AlertService(
            roteador=roteador(), publicador=self.publicador
        )
        self.processador = ProcessadorComandos(self.service)
        self.alerta_id = self.service.processar_evento_roteado(
            evento()
        ).alerta_id

    def enviar(self, dados):
        return json.loads(self.processador.receber(json.dumps(dados)))

    def acao(self, action, **campos):
        payload = {"alert_id": self.alerta_id, "action": action, "actor": "user:maria"}
        payload.update(campos)
        return comando(ACAO, payload)


class TestAlertAction(BaseComandos):

    def test_reply_ok(self):
        dados = self.acao("ACKNOWLEDGE")
        resposta = self.enviar(dados)

        self.assertEqual(resposta["schema"], "ods.s3.alert_action.reply")
        self.assertEqual(resposta["message_type"], "reply")
        self.assertEqual(resposta["correlation_id"], dados["message_id"])
        self.assertEqual(
            resposta["payload"],
            {"status": "OK", "alert_id": self.alerta_id, "state": "ACKNOWLEDGED"},
        )

    def test_mute_e_close(self):
        mute = self.enviar(self.acao("MUTE", mute_until="2030-01-01T00:00:00.000Z"))
        close = self.enviar(self.acao("CLOSE", reason="resolvido"))

        self.assertEqual(mute["payload"]["state"], "MUTED")
        self.assertEqual(close["payload"]["state"], "CLOSED")

        historico = self.service.repository.buscar_historico(self.alerta_id)
        self.assertEqual(historico[-1].motivo, "resolvido")

    def test_mute_sem_prazo(self):
        resposta = self.enviar(self.acao("MUTE"))

        self.assertEqual(resposta["payload"]["status"], "ERROR")
        self.assertEqual(resposta["payload"]["error"]["code"], "INVALID_PAYLOAD")

    def test_mesmo_message_id_mesma_reply(self):
        """Comando repetido: mesma reply, nenhum efeito novo."""

        dados = self.acao("ACKNOWLEDGE")

        primeira = self.enviar(dados)
        segunda = self.enviar(dados)

        self.assertEqual(primeira, segunda)
        self.assertEqual(
            len(self.service.repository.buscar_historico(self.alerta_id)), 2
        )
        # CREATED + ACKNOWLEDGED, sem segunda publicação.
        self.assertEqual(len(self.publicador.mensagens), 2)

    def test_novo_comando_transicao_invalida(self):
        """Outro message_id pedindo o mesmo ACK: INVALID_TRANSITION."""

        self.enviar(self.acao("ACKNOWLEDGE"))
        resposta = self.enviar(self.acao("ACKNOWLEDGE"))

        self.assertEqual(resposta["payload"]["status"], "ERROR")
        self.assertEqual(
            resposta["payload"]["error"]["code"], "INVALID_TRANSITION"
        )

    def test_alerta_inexistente(self):
        dados = self.acao("CLOSE")
        dados["payload"]["alert_id"] = str(uuid4())

        resposta = self.enviar(dados)

        self.assertEqual(resposta["payload"]["error"]["code"], "ALERT_NOT_FOUND")

    def test_alert_id_nao_uuid(self):
        dados = self.acao("CLOSE")
        dados["payload"]["alert_id"] = "nao-existe"

        resposta = self.enviar(dados)

        self.assertEqual(resposta["payload"]["error"]["code"], "INVALID_PAYLOAD")

    def test_acao_desconhecida_ou_sem_actor(self):
        for dados in (
            self.acao("DELETE"),
            self.acao("ACKNOWLEDGE", actor="  "),
            self.acao("ACKNOWLEDGE", actor="maria"),
            self.acao("ACKNOWLEDGE", actor="role:supervisor"),
            self.acao("ACKNOWLEDGE", actor="user:"),
        ):
            with self.subTest(dados=dados["payload"]):
                resposta = self.enviar(dados)
                self.assertEqual(resposta["payload"]["status"], "ERROR")

    def test_exemplo_documentado(self):
        """dados/comando_acao.json silencia o alerta citado."""

        dados = ler_json("comando_acao.json")
        dados["payload"]["alert_id"] = self.alerta_id

        resposta = self.enviar(dados)

        self.assertEqual(resposta["payload"]["state"], "MUTED")


class TestEnvelopeDeComando(BaseComandos):

    def erro_de_envelope(self, dados):
        """Todo comando recebe reply, mesmo com envelope inválido."""

        resposta = self.enviar(dados)

        self.assertEqual(resposta["correlation_id"], dados["message_id"])
        self.assertEqual(resposta["payload"]["status"], "ERROR")
        self.assertEqual(resposta["payload"]["error"]["code"], "INVALID_MESSAGE")

    def test_sem_target(self):
        dados = self.acao("ACKNOWLEDGE")
        del dados["target"]

        self.erro_de_envelope(dados)
        self.assertEqual(self.service.repository.buscar_por_id(self.alerta_id).estado, "CREATED")

    def test_outro_destino(self):
        with self.assertRaises(ErroS3):
            self.enviar(comando(ACAO, {}, target="S4"))

    def test_schema_desconhecido(self):
        with self.assertRaises(ErroS3):
            self.enviar(comando("ods.s4.capture.command", {}))

    def test_message_type_errado(self):
        self.erro_de_envelope(comando(ACAO, {}, message_type="query"))

    def test_outros_erros_de_envelope_tem_reply(self):
        for alteracao in (
            {"schema_version": "2.0"},
            {"published_at": "ontem"},
            {"producer": ""},
        ):
            with self.subTest(alteracao=alteracao):
                self.erro_de_envelope(comando(CRIAR, {}, **alteracao))

        self.erro_de_envelope(comando(CRIAR, []))

    def test_sem_message_id_valido_nao_tem_reply(self):
        """Sem message_id não há correlation_id para a reply."""

        for valor in (None, "cmd-1"):
            with self.subTest(valor=valor):
                with self.assertRaises(ErroS3):
                    self.enviar(comando(ACAO, {}, message_id=valor))

    def test_comando_invalido_repetido_mesma_reply(self):
        dados = comando(ACAO, {}, schema_version="2.0")

        self.assertEqual(self.enviar(dados), self.enviar(dados))


class TestCriarAlerta(BaseComandos):

    def payload(self, **alteracoes):
        dados = ler_json("comando_criar.json")["payload"]
        dados.update(alteracoes)
        return dados

    def test_exemplo_documentado(self):
        resposta = self.enviar(ler_json("comando_criar.json"))

        alerta_id = resposta["payload"]["alert_id"]
        alerta = self.service.repository.buscar_por_id(alerta_id)

        self.assertEqual(resposta["payload"]["status"], "OK")
        self.assertEqual(resposta["schema"], "ods.s3.alert.reply")
        self.assertEqual(alerta.titulo, "Acesso negado")
        self.assertEqual(alerta.destinatarios, ["group:portaria"])

    def test_event_id_e_clip_id_uuid(self):
        """Opcionais, mas com o tipo de ods.s3.alert (UUID)."""

        for campo in ("event_id", "clip_id"):
            with self.subTest(campo=campo):
                resposta = self.enviar(
                    comando(CRIAR, self.payload(**{campo: "evt-001"}))
                )
                self.assertEqual(
                    resposta["payload"]["error"]["code"], "INVALID_PAYLOAD"
                )

                resposta = self.enviar(
                    comando(CRIAR, self.payload(**{campo: str(uuid4())}))
                )
                self.assertEqual(resposta["payload"]["status"], "OK")

    def test_destinatario_invalido(self):
        resposta = self.enviar(comando(CRIAR, self.payload(recipients=["portaria"])))

        self.assertEqual(resposta["payload"]["error"]["code"], "INVALID_RECIPIENT")

    def test_canal_invalido(self):
        resposta = self.enviar(comando(CRIAR, self.payload(channels=["TELA"])))

        self.assertEqual(resposta["payload"]["error"]["code"], "INVALID_CHANNEL")

    def test_campo_obrigatorio_ausente(self):
        for campo in ("title", "message", "severity", "recipients", "channels"):
            with self.subTest(campo=campo):
                payload = self.payload()
                del payload[campo]

                resposta = self.enviar(comando(CRIAR, payload))

                self.assertEqual(resposta["payload"]["status"], "ERROR")

    def test_repetido_cria_um_so(self):
        dados = comando(CRIAR, self.payload())

        primeira = self.enviar(dados)
        segunda = self.enviar(dados)

        self.assertEqual(
            primeira["payload"]["alert_id"], segunda["payload"]["alert_id"]
        )
        alertas, _ = self.service.consultar_alertas()
        self.assertEqual(len(alertas), 2)


class TestConsulta(BaseComandos):

    def test_exemplo_documentado(self):
        resposta = self.enviar(ler_json("consulta_alertas.json"))

        self.assertEqual(resposta["schema"], "ods.s3.alerts.reply")
        self.assertEqual(resposta["payload"]["status"], "OK")
        self.assertEqual(resposta["payload"]["alerts"], [])

    def test_lista_alertas(self):
        resposta = self.enviar(comando(CONSULTA, {"state": "CREATED"}))

        alertas = resposta["payload"]["alerts"]

        self.assertEqual(len(alertas), 1)
        self.assertEqual(alertas[0]["alert_id"], self.alerta_id)
        self.assertNotIn("next_from_at", resposta["payload"])

    def test_paginacao(self):
        self.service.processar_evento_roteado(evento(evento_id="evt-002"))

        resposta = self.enviar(comando(CONSULTA, {"limit": 1}))

        self.assertEqual(len(resposta["payload"]["alerts"]), 1)
        self.assertIn("next_from_at", resposta["payload"])

    def test_filtros_invalidos(self):
        for payload in (
            {"limit": 0},
            {"limit": 501},
            {"state": "CRIADO"},
            {"severity": "ALTA"},
            {"from_at": "ontem"},
        ):
            with self.subTest(payload=payload):
                resposta = self.enviar(comando(CONSULTA, payload))
                self.assertEqual(resposta["payload"]["status"], "ERROR")


if __name__ == "__main__":
    unittest.main()
