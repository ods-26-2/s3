"""
Regressão da Sprint 3, etapa 1: toda mensagem que o S3 recebe
de exemplo ou publica é conferida com contrato.py, que segue a
Especificação 1.0 sem usar as validações internas do S3.
"""

import json
import unittest
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import contrato
from s3.commands import ProcessadorComandos
from s3.errors import ErroS3
from s3.ingestion import IngestorEventos
from s3.service import AlertService

from fabricas import (
    PublicadorFalso,
    comando,
    ler_json,
    mensagem_evento,
    roteador,
)

ACAO = "ods.s3.alert_action.command"
CRIAR = "ods.s3.alert.command"
CONSULTA = "ods.s3.alerts.query"


class BaseS3(unittest.TestCase):

    def setUp(self):
        self.publicador = PublicadorFalso()
        self.service = AlertService(
            roteador=roteador(), publicador=self.publicador
        )
        self.ingestor = IngestorEventos(self.service)
        self.processador = ProcessadorComandos(self.service)

    def evento_s2(self, **payload):
        dados = mensagem_evento(payload)
        self.ingestor.receber(json.dumps(dados))
        return dados

    def enviar(self, dados):
        return json.loads(self.processador.receber(json.dumps(dados)))

    def criar_por_comando(self, **payload):
        dados = ler_json("comando_criar.json")
        dados["message_id"] = str(uuid4())
        dados["payload"].update(payload)
        return dados, self.enviar(dados)

    def acao(self, alerta_id, action, **extra):
        payload = {"alert_id": alerta_id, "action": action, "actor": "user:maria"}
        payload.update(extra)
        return self.enviar(comando(ACAO, payload))


class TestExemplosOficiais(unittest.TestCase):
    """Os arquivos de jsons/ seguem o contrato."""

    def test_entrada_s2(self):
        contrato.envelope(ler_json("entrada.json"), "EV-SERVIÇO")

    def test_saida_alerta(self):
        contrato.mensagem_alerta(ler_json("saida.json"))

    def test_comandos(self):
        for nome in (
            "comando_criar.json",
            "comando_acao.json",
            "consulta_alertas.json",
        ):
            with self.subTest(nome=nome):
                contrato.envelope(ler_json(nome), "CMD")


class TestAlertaDeEvento(BaseS3):
    """Cenário 1: alerta criado a partir de ods.s2.event."""

    def test_envelope_valido_e_contexto_propagado(self):
        entrada = self.evento_s2()

        mensagem = self.publicador.mensagens[0]
        payload = contrato.mensagem_alerta(mensagem)

        self.assertEqual(mensagem["session_id"], entrada["session_id"])
        self.assertEqual(mensagem["captured_at"], entrada["captured_at"])
        self.assertEqual(mensagem["source_id"], entrada["source_id"])
        self.assertEqual(mensagem["correlation_id"], entrada["payload"]["event_id"])
        self.assertEqual(mensagem["artifact_version"], "routing-a5-1")
        self.assertEqual(mensagem["sequence"], 0)
        self.assertEqual(payload["event_id"], entrada["payload"]["event_id"])
        self.assertEqual(payload["state"], "CREATED")

    def test_republicacoes_mantem_correlacao_e_sessao(self):
        """Cenários 6 e 7: ACK, MUTE e CLOSE republicam o alerta."""

        entrada = self.evento_s2()
        alerta_id = self.publicador.mensagens[0]["payload"]["alert_id"]

        respostas = [
            self.acao(alerta_id, "ACKNOWLEDGE"),
            self.acao(alerta_id, "MUTE", mute_until="2030-01-01T00:00:00.000Z"),
            self.acao(alerta_id, "CLOSE", reason="EPI colocado"),
        ]

        for resposta, estado in zip(respostas, ("ACKNOWLEDGED", "MUTED", "CLOSED")):
            contrato.envelope(resposta, "RPL")
            self.assertEqual(
                resposta["payload"],
                {"status": "OK", "alert_id": alerta_id, "state": estado},
            )

        mensagens = self.publicador.mensagens

        self.assertEqual(
            [m["payload"]["state"] for m in mensagens],
            ["CREATED", "ACKNOWLEDGED", "MUTED", "CLOSED"],
        )
        self.assertEqual([m["sequence"] for m in mensagens], [0, 1, 2, 3])

        for mensagem in mensagens:
            contrato.mensagem_alerta(mensagem)
            self.assertEqual(mensagem["session_id"], entrada["session_id"])
            self.assertEqual(
                mensagem["correlation_id"], entrada["payload"]["event_id"]
            )

        final = mensagens[-1]["payload"]

        self.assertEqual(final["acknowledged_by"], "user:maria")
        self.assertEqual(final["muted_until"], "2030-01-01T00:00:00.000Z")
        self.assertEqual(final["closed_by"], "user:maria")
        self.assertIn("acknowledged_at", final)
        self.assertIn("closed_at", final)

    def test_produtor_diferente_de_s2_rejeitado(self):
        """Só o S2 produz ods.s2.event (seção 9)."""

        dados = mensagem_evento(producer="A5")

        with self.assertRaises(ErroS3):
            self.ingestor.receber(json.dumps(dados))

        self.assertEqual(self.publicador.mensagens, [])


class TestAlertaDeComando(BaseS3):
    """Cenário 2: alerta de negócio, sem event_id."""

    def test_reply_e_payload_validos(self):
        dados, resposta = self.criar_por_comando()

        contrato.envelope(resposta, "RPL")
        self.assertEqual(resposta["schema"], "ods.s3.alert.reply")
        self.assertEqual(resposta["correlation_id"], dados["message_id"])
        self.assertEqual(resposta["payload"]["status"], "OK")

        mensagem = self.publicador.mensagens[0]
        payload = mensagem["payload"]

        contrato.payload_alerta(payload)
        self.assertEqual(payload["alert_id"], resposta["payload"]["alert_id"])
        self.assertNotIn("event_id", payload)
        self.assertEqual(mensagem["correlation_id"], dados["message_id"])
        self.assertNotIn("artifact_version", mensagem)
        self.assertNotIn("source_id", mensagem)

    def test_sem_session_id_registra_aviso(self):
        """O S3 não inventa sessão; avisa no log (pendência 5)."""

        with self.assertLogs("s3.service", level="WARNING") as registro:
            self.criar_por_comando()

        self.assertIn("sem session_id", registro.output[0])
        self.assertNotIn("session_id", self.publicador.mensagens[0])

    @unittest.expectedFailure
    def test_envelope_ev_servico_completo(self):
        """
        Pendência 5 (TRA-1, TRA-2): EV-SERVIÇO exige session_id,
        e um alerta criado por comando não tem sessão de origem.
        Este teste passa a "passar inesperadamente" quando a
        decisão for implementada; aí, retire o expectedFailure.
        """

        self.criar_por_comando()

        contrato.mensagem_alerta(self.publicador.mensagens[0])

    def test_event_id_opcional_informado(self):
        event_id = str(uuid4())
        self.criar_por_comando(event_id=event_id)

        payload = self.publicador.mensagens[0]["payload"]

        contrato.payload_alerta(payload)
        self.assertEqual(payload["event_id"], event_id)

    def test_valores_invalidos(self):
        """Cenários 5 e 8: erros oficiais e provisórios."""

        for campo, valor, codigo in (
            ("recipients", ["portaria"], "INVALID_RECIPIENT"),
            ("recipients", "role:supervisor", "INVALID_RECIPIENT"),
            ("recipients", [], "INVALID_RECIPIENT"),
            ("channels", ["EMAIL"], "INVALID_CHANNEL"),
            ("channels", [], "INVALID_CHANNEL"),
            ("severity", "CRITICAL", "INVALID_PAYLOAD"),
            ("severity", "high", "INVALID_PAYLOAD"),
        ):
            with self.subTest(campo=campo, valor=valor):
                _, resposta = self.criar_por_comando(**{campo: valor})

                contrato.envelope(resposta, "RPL")
                self.assertEqual(resposta["payload"]["error"]["code"], codigo)

        self.assertEqual(self.publicador.mensagens, [])


class TestErrosDeAcao(BaseS3):

    def setUp(self):
        super().setUp()
        self.evento_s2()
        self.alerta_id = self.publicador.mensagens[0]["payload"]["alert_id"]

    def codigo(self, resposta):
        contrato.envelope(resposta, "RPL")
        return resposta["payload"]["error"]["code"]

    def test_alerta_inexistente(self):
        self.assertEqual(
            self.codigo(self.acao(str(uuid4()), "ACKNOWLEDGE")),
            "ALERT_NOT_FOUND",
        )

    def test_transicao_invalida_nao_publica(self):
        self.acao(self.alerta_id, "CLOSE")

        self.assertEqual(
            self.codigo(self.acao(self.alerta_id, "ACKNOWLEDGE")),
            "INVALID_TRANSITION",
        )
        self.assertEqual(len(self.publicador.mensagens), 2)

    def test_mute_sem_mute_until(self):
        self.assertEqual(
            self.codigo(self.acao(self.alerta_id, "MUTE")),
            "INVALID_PAYLOAD",
        )

    def test_envelope_invalido_tem_reply(self):
        """Seção 2, regra 6: todo comando recebe reply."""

        dados = comando(ACAO, {}, schema_version="2.0")
        resposta = self.enviar(dados)

        self.assertEqual(self.codigo(resposta), "INVALID_MESSAGE")
        self.assertEqual(resposta["correlation_id"], dados["message_id"])


class TestConsulta(BaseS3):
    """Cenário 9: filtros e paginação."""

    def setUp(self):
        super().setUp()

        for _ in range(5):
            self.evento_s2(event_id=str(uuid4()))

        self.criar_por_comando()

        # Horários de criação fixos, 1 s entre alertas, para
        # o teste não depender da velocidade da máquina.
        alertas = self.service.repository._alertas.values()
        inicio = datetime(2026, 9, 23, 14, 0, tzinfo=timezone.utc)

        for posicao, alerta in enumerate(alertas):
            alerta.criado_em = inicio + timedelta(seconds=posicao)

    def consultar(self, **payload):
        resposta = self.enviar(comando(CONSULTA, payload))
        contrato.envelope(resposta, "RPL")
        self.assertEqual(resposta["schema"], "ods.s3.alerts.reply")
        return resposta["payload"]

    def test_itens_seguem_ods_s3_alert(self):
        corpo = self.consultar()

        self.assertEqual(len(corpo["alerts"]), 6)
        self.assertNotIn("next_from_at", corpo)

        for item in corpo["alerts"]:
            contrato.payload_alerta(item)

    def test_filtros(self):
        self.assertEqual(len(self.consultar(severity="HIGH")["alerts"]), 5)
        self.assertEqual(len(self.consultar(severity="MEDIUM")["alerts"]), 1)
        self.assertEqual(len(self.consultar(state="CLOSED")["alerts"]), 0)
        self.assertEqual(
            len(self.consultar(to_at="2000-01-01T00:00:00.000Z")["alerts"]), 0
        )

    def test_paginacao_completa_sem_repeticao(self):
        """
        Percorre o histórico por next_from_at. Alertas criados
        no mesmo milissegundo não podem aparecer duas vezes.
        """

        vistos = []
        payload = {"limit": 2}

        for _ in range(10):
            corpo = self.consultar(**payload)
            vistos += [item["alert_id"] for item in corpo["alerts"]]

            if "next_from_at" not in corpo:
                break

            contrato._horario(corpo["next_from_at"], "next_from_at")
            payload = {"limit": 2, "from_at": corpo["next_from_at"]}

        self.assertEqual(len(vistos), 6)
        self.assertEqual(len(set(vistos)), 6)

    def test_limite_maximo(self):
        for limite in (0, 501, "100", True):
            with self.subTest(limite=limite):
                resposta = self.enviar(comando(CONSULTA, {"limit": limite}))
                self.assertEqual(
                    resposta["payload"]["error"]["code"], "INVALID_PAYLOAD"
                )

        self.assertEqual(self.consultar(limit=500)["alerts"].__len__(), 6)


class TestPaginacaoMesmoMilissegundo(unittest.TestCase):
    """next_from_at tem milissegundos; a criação, microssegundos."""

    def preparar(self, *microssegundos):
        from s3.alert import Alerta
        from s3.repository import AlertRepository

        repository = AlertRepository()
        base = datetime(2026, 9, 23, 14, 0, 0, tzinfo=timezone.utc)

        for micro in microssegundos:
            repository.salvar(
                Alerta(
                    alerta_id=str(uuid4()),
                    severidade="HIGH",
                    destinatarios=["role:supervisor"],
                    canais=["SCREEN"],
                    titulo="t",
                    mensagem="m",
                    criado_em=base.replace(microsecond=micro),
                ),
                indexar_evento=False,
            )

        return repository, base

    def test_pagina_termina_antes_do_milissegundo(self):
        """
        O 2º e o 3º alertas estão no milissegundo .500: a primeira
        página para antes dele, e a segunda começa nele.
        """

        repository, base = self.preparar(499000, 500100, 500200, 502000)

        primeira, proximo = repository.listar(limite=2)

        self.assertEqual(len(primeira), 1)
        self.assertEqual(proximo, base.replace(microsecond=500000))

        segunda, proximo = repository.listar(desde=proximo, limite=2)

        self.assertEqual(len(segunda), 2)
        self.assertEqual(proximo, base.replace(microsecond=502000))

        terceira, proximo = repository.listar(desde=proximo, limite=2)

        self.assertEqual(len(terceira), 1)
        self.assertIsNone(proximo)

        ids = [a.alerta_id for a in primeira + segunda + terceira]
        self.assertEqual(len(set(ids)), 4)

    def test_mais_alertas_que_o_limite_no_mesmo_milissegundo(self):
        """
        Limitação documentada: sem cursor no contrato, a página
        não avança dentro de um mesmo milissegundo.
        """

        repository, base = self.preparar(500100, 500200, 500300)

        pagina, proximo = repository.listar(limite=2)

        self.assertEqual(len(pagina), 2)
        self.assertEqual(proximo, base.replace(microsecond=500000))


class TestFalhaDePublicacao(unittest.TestCase):
    """A falha do barramento não quebra a idempotência."""

    class PublicadorComFalha:
        def publicar(self, mensagem):
            raise ConnectionError("barramento indisponível")

    def test_comando_repetido_nao_duplica_alerta(self):
        service = AlertService(publicador=self.PublicadorComFalha())
        processador = ProcessadorComandos(service)

        dados = json.dumps(ler_json("comando_criar.json"))

        with self.assertLogs("s3.service", level="ERROR"):
            primeira = json.loads(processador.receber(dados))

        segunda = json.loads(processador.receber(dados))

        self.assertEqual(primeira, segunda)
        self.assertEqual(primeira["payload"]["status"], "OK")

        alertas, _ = service.consultar_alertas()
        self.assertEqual(len(alertas), 1)


class TestRepublicacaoEOrdem(BaseS3):
    """
    O que o consumidor recebe para separar repetição de atualização
    (CHECKLIST_INTEGRACAO_S3.md, seção 2).
    """

    def test_cada_republicacao_tem_message_id_novo(self):
        """
        Mesmo alert_id ao longo do ciclo; message_id novo e sequence
        crescente na mesma sessão a cada mudança de estado.
        """

        entrada = self.evento_s2()
        alerta_id = self.publicador.mensagens[0]["payload"]["alert_id"]

        self.acao(alerta_id, "ACKNOWLEDGE")
        self.acao(alerta_id, "CLOSE")

        mensagens = self.publicador.mensagens

        self.assertEqual({m["payload"]["alert_id"] for m in mensagens}, {alerta_id})
        self.assertEqual(len({m["message_id"] for m in mensagens}), 3)
        self.assertEqual({m["session_id"] for m in mensagens}, {entrada["session_id"]})
        self.assertEqual([m["sequence"] for m in mensagens], [0, 1, 2])

    def test_ended_antes_de_started(self):
        """
        Comportamento atual, registrado como risco (pendência 6):
        se o ENDED chegar antes do STARTED, o ENDED é ignorado e o
        STARTED cria um alerta para uma violação que já terminou.
        """

        event_id = str(uuid4())

        with self.assertLogs("s3.service", level="WARNING"):
            self.evento_s2(event_id=event_id, phase="ENDED")

        self.evento_s2(event_id=event_id, phase="STARTED")

        self.assertEqual(
            [m["payload"]["state"] for m in self.publicador.mensagens],
            ["CREATED"],
        )


if __name__ == "__main__":
    unittest.main()
