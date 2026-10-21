"""
Persistência em SQLite e fila de saída (transactional outbox).

1. Os testes já existentes rodam de novo com o SqliteAlertRepository:
   as duas versões do repositório seguem as mesmas regras.
2. Testes novos: reinício, falha de publicação, reenvio, ordem e
   atomicidade entre a mudança de estado e a mensagem a publicar.
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

import test_clipes
import test_commands
import test_contratos_saida
import test_repository
import test_service
from s3.commands import ProcessadorComandos
from s3.ingestion import IngestorEventos
from s3.kafka_adapter import AssinanteKafka
from s3.repository_sqlite import SqliteAlertRepository
from s3.service import AlertService

from fabricas import (
    FUTURO,
    PublicadorFalso,
    comando,
    ler_json,
    mensagem_evento,
    roteador,
)

ACAO = "ods.s3.alert_action.command"


# ----------------------------------------------------------------
# 1. Testes existentes, agora com SQLite


def _sqlite_em_memoria(*_args, **_kwargs):
    return SqliteAlertRepository(":memory:")


def _com_sqlite(base, alvo):
    """Subclasse do teste base com AlertRepository trocado pelo SQLite."""

    class ComSqlite(base):
        def setUp(self):
            troca = mock.patch(alvo, side_effect=_sqlite_em_memoria)
            troca.start()
            self.addCleanup(troca.stop)
            super().setUp()

    ComSqlite.__name__ = ComSqlite.__qualname__ = base.__name__ + "Sqlite"
    return ComSqlite


_REPETIDOS = [
    (test_repository, "test_repository.AlertRepository"),
    (test_service, "s3.service.AlertRepository"),
    (test_commands, "s3.service.AlertRepository"),
    (test_contratos_saida, "s3.service.AlertRepository"),
    (test_clipes, "s3.service.AlertRepository"),
]

for _modulo, _alvo in _REPETIDOS:
    for _nome in dir(_modulo):
        _classe = getattr(_modulo, _nome)

        if (
            isinstance(_classe, type)
            and issubclass(_classe, unittest.TestCase)
            and _classe.__module__ == _modulo.__name__
            and _nome.startswith("Test")
        ):
            globals()[_nome + "Sqlite"] = _com_sqlite(_classe, _alvo)


# ----------------------------------------------------------------
# 2. Testes novos


class PublicadorQueFalha:
    """Barramento fora do ar."""

    def __init__(self):
        self.tentativas = 0

    def publicar(self, mensagem):
        self.tentativas += 1
        raise ConnectionError("barramento indisponível")


class BaseArquivo(unittest.TestCase):
    """Banco num arquivo de verdade, para simular o reinício."""

    def setUp(self):
        self.pasta = tempfile.mkdtemp()
        self.caminho = os.path.join(self.pasta, "s3.sqlite3")
        self.repositorios = []
        self.addCleanup(self._limpar)

    def _limpar(self):
        for repositorio in self.repositorios:
            repositorio.fechar()

        shutil.rmtree(self.pasta, ignore_errors=True)

    def abrir(self, publicador=None):
        """Abre (ou reabre, como num reinício) o S3 sobre o mesmo banco."""

        repositorio = SqliteAlertRepository(self.caminho)
        self.repositorios.append(repositorio)

        service = AlertService(
            repository=repositorio,
            roteador=roteador(captura=True),
            publicador=publicador if publicador is not None else PublicadorFalso(),
        )

        return service, ProcessadorComandos(service), IngestorEventos(service)

    def reiniciar(self, publicador=None):
        self.repositorios[-1].fechar()
        self.repositorios.pop()
        return self.abrir(publicador)


class TestReinicio(BaseArquivo):

    def test_alertas_e_historico_sobrevivem(self):
        service, processador, ingestor = self.abrir()
        alerta = ingestor.receber(json.dumps(mensagem_evento()))
        service.reconhecer_alerta(alerta.alerta_id, "user:maria", "vi")
        service.silenciar_alerta(alerta.alerta_id, "user:maria", FUTURO)

        service, _, _ = self.reiniciar()

        recuperado = service.repository.buscar_por_id(alerta.alerta_id)
        self.assertEqual(recuperado.estado, "MUTED")
        self.assertEqual(recuperado.reconhecido_por, "user:maria")
        self.assertEqual(recuperado.silenciado_ate, FUTURO)
        self.assertEqual(recuperado.criado_em, alerta.criado_em)
        self.assertTrue(recuperado.captura)

        historico = service.repository.buscar_historico(alerta.alerta_id)
        self.assertEqual([r.acao for r in historico], ["CREATED", "ACKNOWLEDGED", "MUTED"])
        self.assertEqual(historico[1].motivo, "vi")

        alertas, _ = service.consultar_alertas()
        self.assertEqual(len(alertas), 1)

    def test_deduplicacao_sobrevive(self):
        """
        O mesmo ods.s2.event e o mesmo comando, entregues de novo
        depois do reinício, não criam nada novo.
        """

        _, processador, ingestor = self.abrir()
        evento = json.dumps(mensagem_evento())
        dados = json.dumps(ler_json("comando_criar.json"))

        ingestor.receber(evento)
        primeira = processador.receber(dados)

        service, processador, ingestor = self.reiniciar()

        self.assertIsNone(ingestor.receber(evento))
        self.assertEqual(processador.receber(dados), primeira)

        alertas, _ = service.consultar_alertas()
        self.assertEqual(len(alertas), 2)

    def test_sequence_continua_depois_do_reinicio(self):
        """O sequence não volta a 0 na mesma sessão (seção 3)."""

        publicador = PublicadorFalso()
        service, _, ingestor = self.abrir(publicador)
        alerta = ingestor.receber(json.dumps(mensagem_evento()))
        service.reconhecer_alerta(alerta.alerta_id, "user:maria")

        publicador = PublicadorFalso()
        service, _, _ = self.reiniciar(publicador)
        service.encerrar_alerta(alerta.alerta_id, "user:maria")

        self.assertEqual(publicador.mensagens[0]["sequence"], 2)

    def test_clipe_pendente_sobrevive(self):
        from s3.ingestion import IngestorClipes
        from test_clipes import mensagem_clipe

        service, _, _ = self.abrir()
        evento = mensagem_evento()
        clipe = mensagem_clipe({"event_id": evento["payload"]["event_id"]})
        IngestorClipes(service).receber(json.dumps(clipe))

        publicador = PublicadorFalso()
        _, _, ingestor = self.reiniciar(publicador)
        ingestor.receber(json.dumps(evento))

        self.assertEqual(
            publicador.mensagens[0]["payload"]["clip_id"],
            clipe["payload"]["clip_id"],
        )


class TestFilaDeSaida(BaseArquivo):

    def test_falha_de_publicacao_fica_na_fila_e_sai_depois(self):
        """Antes: a mensagem se perdia. Agora sai quando o barramento volta."""

        falha = PublicadorQueFalha()
        service, _, ingestor = self.abrir(falha)

        with self.assertLogs("s3.service", level="ERROR"):
            alerta = ingestor.receber(json.dumps(mensagem_evento()))

        self.assertEqual(len(service.repository.pendentes_saida()), 1)

        publicador = PublicadorFalso()
        service, _, _ = self.reiniciar(publicador)

        self.assertEqual(service.despachar_saida(), 1)
        self.assertEqual(publicador.mensagens[0]["payload"]["alert_id"], alerta.alerta_id)
        self.assertEqual(service.repository.pendentes_saida(), [])

    def test_ordem_preservada(self):
        """Se a primeira falha, as seguintes esperam: a ordem não se inverte."""

        falha = PublicadorQueFalha()
        service, _, ingestor = self.abrir(falha)

        with self.assertLogs("s3.service", level="ERROR"):
            alerta = ingestor.receber(json.dumps(mensagem_evento()))
            service.reconhecer_alerta(alerta.alerta_id, "user:maria")
            service.encerrar_alerta(alerta.alerta_id, "user:maria")

        self.assertEqual(len(service.repository.pendentes_saida()), 3)

        publicador = PublicadorFalso()
        service.publicador = publicador
        service.despachar_saida()

        self.assertEqual(
            [m["payload"]["state"] for m in publicador.mensagens],
            ["CREATED", "ACKNOWLEDGED", "CLOSED"],
        )
        self.assertEqual([m["sequence"] for m in publicador.mensagens], [0, 1, 2])

    def test_entrega_pelo_menos_uma_vez_com_o_mesmo_message_id(self):
        """
        O processo cai depois de publicar e antes de marcar como
        enviada: a mensagem sai de novo, com o mesmo message_id, e o
        consumidor a descarta (seção 2, regra 7).
        """

        primeiro = PublicadorFalso()
        service, _, ingestor = self.abrir(primeiro)

        with mock.patch.object(
            service.repository, "marcar_enviada", side_effect=RuntimeError("queda")
        ):
            with self.assertRaises(RuntimeError):
                ingestor.receber(json.dumps(mensagem_evento()))

        self.assertEqual(len(primeiro.mensagens), 1)

        segundo = PublicadorFalso()
        service, _, _ = self.reiniciar(segundo)
        service.despachar_saida()

        self.assertEqual(len(segundo.mensagens), 1)
        self.assertEqual(
            segundo.mensagens[0]["message_id"], primeiro.mensagens[0]["message_id"]
        )
        self.assertEqual(service.repository.pendentes_saida(), [])

    def test_mesmo_message_id_no_reenvio(self):
        falha = PublicadorQueFalha()
        service, _, ingestor = self.abrir(falha)

        with self.assertLogs("s3.service", level="ERROR"):
            ingestor.receber(json.dumps(mensagem_evento()))

        guardada = json.loads(service.repository.pendentes_saida()[0][1])

        publicador = PublicadorFalso()
        service, _, _ = self.reiniciar(publicador)
        service.despachar_saida()

        self.assertEqual(publicador.mensagens[0]["message_id"], guardada["message_id"])


class TestAtomicidade(BaseArquivo):

    def test_falha_ao_enfileirar_desfaz_o_alerta(self):
        """Alerta e mensagem a publicar: ou os dois, ou nenhum."""

        service, _, ingestor = self.abrir()

        with mock.patch.object(
            service.repository, "enfileirar_saida", side_effect=RuntimeError("disco")
        ):
            with self.assertRaises(RuntimeError):
                ingestor.receber(json.dumps(mensagem_evento()))

        self.assertEqual(service.consultar_alertas()[0], [])

        service, _, ingestor = self.reiniciar()

        self.assertEqual(service.consultar_alertas()[0], [])
        self.assertIsNotNone(ingestor.receber(json.dumps(mensagem_evento())))

    def test_queda_antes_de_registrar_o_comando_nao_duplica(self):
        """
        O processo cai depois de criar o alerta e antes de guardar a
        reply. Antes da outbox, a reentrega criava um segundo alerta.
        """

        service, processador, _ = self.abrir()
        dados = json.dumps(ler_json("comando_criar.json"))

        with mock.patch.object(
            service.repository, "registrar_mensagem", side_effect=RuntimeError("queda")
        ):
            with self.assertRaises(RuntimeError):
                processador.receber(dados)

        service, processador, _ = self.reiniciar()
        resposta = json.loads(processador.receber(dados))

        self.assertEqual(resposta["payload"]["status"], "OK")
        self.assertEqual(len(service.consultar_alertas()[0]), 1)

    def test_erro_de_regra_nao_desfaz_a_reply(self):
        """INVALID_TRANSITION: nada muda, mas a reply de erro fica guardada."""

        service, processador, ingestor = self.abrir()
        alerta = ingestor.receber(json.dumps(mensagem_evento()))
        service.encerrar_alerta(alerta.alerta_id, "user:maria")

        dados = json.dumps(comando(ACAO, {"alert_id": alerta.alerta_id, "action": "ACKNOWLEDGE", "actor": "user:x"}))
        resposta = json.loads(processador.receber(dados))

        service, processador, _ = self.reiniciar()

        self.assertEqual(resposta["payload"]["error"]["code"], "INVALID_TRANSITION")
        self.assertEqual(json.loads(processador.receber(dados)), resposta)
        self.assertEqual(service.repository.buscar_por_id(alerta.alerta_id).estado, "CLOSED")


class TestPeriodicaNoKafka(unittest.TestCase):

    def test_reenvio_a_cada_volta_do_laco(self):
        """O laço do consumidor chama a tarefa periódica mesmo sem mensagem."""

        class Consumidor:
            def subscribe(self, topicos):
                pass

            def poll(self, intervalo):
                return None

            def close(self):
                pass

        chamadas = []
        assinante = AssinanteKafka(consumidor=Consumidor())
        assinante.assinar(lambda mensagem: None)

        def periodica():
            chamadas.append(1)
            if len(chamadas) >= 3:
                assinante.parar()

        assinante.executar(tempo_limite_s=5, intervalo_poll_s=0, periodica=periodica)

        self.assertEqual(len(chamadas), 3)


class TestPartida(BaseArquivo):

    def test_main_reenvia_a_fila_na_partida(self):
        """python -m s3 publica o que ficou pendente antes da queda."""

        import s3.__main__ as principal

        falha = PublicadorQueFalha()
        _, _, ingestor = self.abrir(falha)

        with self.assertLogs("s3.service", level="ERROR"):
            ingestor.receber(json.dumps(mensagem_evento()))

        self.repositorios[-1].fechar()
        self.repositorios.pop()

        publicadas = []

        class Publicador:
            def publicar(self, mensagem):
                publicadas.append(json.loads(mensagem)["schema"])

        class Assinante:
            def __init__(self, topicos):
                pass

            def assinar(self, callback):
                pass

            def executar(self, **opcoes):
                return 0

        argv = ["s3", "--banco", self.caminho, "--max-mensagens", "1"]

        with mock.patch.object(principal, "PublicadorKafka", Publicador), \
                mock.patch.object(principal, "AssinanteKafka", Assinante), \
                mock.patch.object(sys, "argv", argv):
            principal.main()

        self.assertEqual(publicadas, ["ods.s3.alert"])


if __name__ == "__main__":
    unittest.main()
