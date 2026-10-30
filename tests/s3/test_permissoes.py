"""
Autorização do actor (Sprint 3, S3.4: "ações inválidas ou não
autorizadas são rejeitadas"). Extensão do VER-3: arquivo de
permissões por build e código UNAUTHORIZED_ACTOR.
"""

import json
import unittest
from uuid import uuid4

from s3.commands import ProcessadorComandos
from s3.ingestion import IngestorEventos
from s3.permissoes import PoliticaPermissoes
from s3.service import AlertService

from fabricas import PublicadorFalso, comando, ler_json, mensagem_evento, roteador

ACAO = "ods.s3.alert_action.command"


def politica(**alteracoes):
    conteudo = ler_json("permissoes_a5.json")
    conteudo.update(alteracoes)
    return PoliticaPermissoes.de_artefato(conteudo, "permissoes-a5-1")


class TestPolitica(unittest.TestCase):

    def test_exemplo_da_build_do_a5(self):
        p = politica()

        self.assertTrue(p.permite("user:maria", "CLOSE"))
        self.assertTrue(p.permite("user:ana", "ACKNOWLEDGE"))
        self.assertFalse(p.permite("user:ana", "CLOSE"))
        self.assertFalse(p.permite("user:visitante", "ACKNOWLEDGE"))

    def test_usuario_direto_e_qualquer_um(self):
        p = politica(actions={"ACKNOWLEDGE": ["*"], "CLOSE": ["user:ana"]})

        self.assertTrue(p.permite("user:qualquer", "ACKNOWLEDGE"))
        self.assertTrue(p.permite("user:ana", "CLOSE"))
        self.assertFalse(p.permite("user:maria", "CLOSE"))

    def test_acao_fora_do_arquivo_e_negada(self):
        p = politica(actions={"ACKNOWLEDGE": ["*"]})

        self.assertFalse(p.permite("user:maria", "MUTE"))

    def test_arquivo_invalido_impede_a_partida(self):
        for conteudo in (
            [],
            {"members": {}},
            {"actions": {"DELETE": ["*"]}},
            {"actions": {"CLOSE": []}},
            {"actions": {"CLOSE": ["maria"]}},
            {"actions": {"CLOSE": ["role:inexistente"]}},
            {"members": {"supervisor": ["user:maria"]}, "actions": {"CLOSE": ["*"]}},
            {"members": {"role:supervisor": ["maria"]}, "actions": {"CLOSE": ["role:supervisor"]}},
        ):
            with self.subTest(conteudo=conteudo):
                with self.assertRaises(ValueError):
                    PoliticaPermissoes.de_artefato(conteudo)


class TestComandoComPermissoes(unittest.TestCase):

    def setUp(self):
        self.publicador = PublicadorFalso()
        self.service = AlertService(roteador=roteador(), publicador=self.publicador)
        IngestorEventos(self.service).receber(json.dumps(mensagem_evento()))
        self.alerta_id = self.publicador.mensagens[0]["payload"]["alert_id"]
        self.processador = ProcessadorComandos(self.service, politica())

    def acao(self, action, actor, **extra):
        payload = {"alert_id": self.alerta_id, "action": action, "actor": actor, **extra}
        dados = comando(ACAO, payload)
        return dados, json.loads(self.processador.receber(json.dumps(dados)))

    def test_autorizado(self):
        _, resposta = self.acao("ACKNOWLEDGE", "user:ana")

        self.assertEqual(resposta["payload"]["state"], "ACKNOWLEDGED")

    def test_nao_autorizado_e_rejeitado_sem_efeito(self):
        _, resposta = self.acao("CLOSE", "user:ana")

        self.assertEqual(resposta["payload"]["status"], "ERROR")
        self.assertEqual(resposta["payload"]["error"]["code"], "UNAUTHORIZED_ACTOR")
        self.assertEqual(self.service.repository.buscar_por_id(self.alerta_id).estado, "CREATED")
        self.assertEqual(len(self.publicador.mensagens), 1)

        historico = self.service.repository.buscar_historico(self.alerta_id)
        self.assertEqual([r.acao for r in historico], ["CREATED"])

    def test_resposta_repetida_e_a_mesma(self):
        dados, resposta = self.acao("MUTE", "user:visitante", mute_until="2030-01-01T00:00:00.000Z")

        self.assertEqual(json.loads(self.processador.receber(json.dumps(dados))), resposta)

    def test_nao_revela_se_o_alerta_existe(self):
        """Sem permissão, a resposta é a mesma para alerta existente ou não."""

        self.alerta_id = str(uuid4())
        _, resposta = self.acao("CLOSE", "user:visitante")

        self.assertEqual(resposta["payload"]["error"]["code"], "UNAUTHORIZED_ACTOR")

    def test_formato_errado_continua_invalid_payload(self):
        _, resposta = self.acao("CLOSE", "maria")

        self.assertEqual(resposta["payload"]["error"]["code"], "INVALID_PAYLOAD")

    def test_exemplo_oficial_de_mute_e_autorizado(self):
        """O exemplo da especificação (user:maria silencia) passa com a política do A5."""

        dados = ler_json("comando_acao.json")
        dados["payload"]["alert_id"] = self.alerta_id
        resposta = json.loads(self.processador.receber(json.dumps(dados)))

        self.assertEqual(resposta["payload"]["state"], "MUTED")

    def test_sem_politica_nada_muda(self):
        processador = ProcessadorComandos(self.service)
        dados = comando(ACAO, {"alert_id": self.alerta_id, "action": "CLOSE", "actor": "user:visitante"})

        resposta = json.loads(processador.receber(json.dumps(dados)))

        self.assertEqual(resposta["payload"]["state"], "CLOSED")


if __name__ == "__main__":
    unittest.main()
