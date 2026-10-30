
import json
import logging

from s3.alert import ESTADOS, SEVERIDADES
from s3.envelope import (
    PERFIL_CMD,
    decodificar,
    formatar_horario,
    ler_horario,
    montar_envelope,
    validar_envelope,
    validar_uuid,
)
from s3.errors import INVALID_MESSAGE, INVALID_PAYLOAD, ErroS3
from s3.output import payload_alerta
from s3.permissoes import PoliticaPermissoes
from s3.rastreio import registrar
from s3.service import AlertService

"comandos e consultas"
'Atende ods.s3.alert.command, ods.s3.alert_action.command e ods.s3.alerts.query'

logger = logging.getLogger(__name__)

SCHEMA_CRIAR = "ods.s3.alert.command"
SCHEMA_ACAO = "ods.s3.alert_action.command"
SCHEMA_CONSULTA = "ods.s3.alerts.query"

DESTINO = "S3"

LIMITE_PADRAO = 100
LIMITE_MAXIMO = 500


def _texto(payload: dict, campo: str, obrigatorio: bool = True):
    valor = payload.get(campo)

    if valor is None and not obrigatorio:
        return None

    if not isinstance(valor, str) or not valor.strip():
        raise ErroS3(INVALID_PAYLOAD, f"{campo} deve ser um texto não vazio.")

    return valor


def _identificador(payload: dict, campo: str, obrigatorio: bool = True):
    """UUID v4, como alert_id, event_id e clip_id."""

    valor = payload.get(campo)

    if valor is None and not obrigatorio:
        return None

    validar_uuid(valor, campo)

    return valor


def _horario(payload: dict, campo: str, obrigatorio: bool = True):
    valor = payload.get(campo)

    if valor is None and not obrigatorio:
        return None

    return ler_horario(valor, campo)


def _schema_reply(schema: str) -> str:
    """ods.s3.alert_action.command → ods.s3.alert_action.reply"""

    return schema.rsplit(".", 1)[0] + ".reply"


class ProcessadorComandos:
    """
    Recebe comandos e consultas dirigidos ao S3
    e devolve a reply (texto JSON) a publicar.

    Idempotência pelo message_id: um comando repetido
    recebe a mesma reply, sem novo efeito.
    """

    def __init__(
        self,
        service: AlertService,
        permissoes: PoliticaPermissoes | None = None,
    ) -> None:
        self.service = service

        # Sem política, qualquer actor no formato user:<id> pode agir
        # (a aplicação confere antes de enviar).
        self.permissoes = permissoes

        self._tratadores = {
            SCHEMA_CRIAR: ("command", self._criar),
            SCHEMA_ACAO: ("command", self._acao),
            SCHEMA_CONSULTA: ("query", self._consultar),
        }

    def receber(self, mensagem: str | bytes) -> str:
        """
        Processa um comando ou consulta.

        Todo comando recebe reply (seção 2, regra 6): erro
        no envelope, no payload ou na regra vira reply com
        status ERROR. Só gera ErroS3, sem reply, a mensagem
        que não é do S3 (outro schema ou outro target) ou
        que não tem message_id válido para a correlação.
        """

        dados = decodificar(mensagem)
        schema = dados.get("schema")

        if schema not in self._tratadores:
            raise ErroS3(INVALID_MESSAGE, f"schema inesperado: {schema}")

        message_type, tratador = self._tratadores[schema]

        target = dados.get("target")

        if target is not None and target != DESTINO:
            raise ErroS3(
                INVALID_MESSAGE, f"Comando para outro destino: {target}"
            )

        message_id = dados.get("message_id")
        validar_uuid(message_id, "message_id", INVALID_MESSAGE)

        repository = self.service.repository

        anterior = repository.resposta_registrada(message_id)

        if anterior is not None:
            registrar(logger, "entrada", dados, resultado="repetida")
            return json.dumps(anterior, ensure_ascii=False)

        # O efeito do comando, a fila de saída e a reply registrada
        # são gravados juntos: se o processo cair no meio, nada fica,
        # e o comando reentregue é processado do zero, sem duplicar.
        # Um erro de regra desfaz só a parte do tratador (transação
        # aninhada no serviço); a reply de erro é registrada.
        with repository.transacao():
            try:
                payload = validar_envelope(
                    dados, schema, message_type, PERFIL_CMD
                )
                corpo = tratador(payload, message_id)
            except ErroS3 as erro:
                corpo = self._erro(erro.codigo, str(erro))
            except ValueError as erro:
                corpo = self._erro(INVALID_PAYLOAD, str(erro))

            resposta = montar_envelope(
                "reply",
                _schema_reply(schema),
                corpo,
                correlation_id=message_id,
            )

            # Consultas não alteram estado; só comandos
            # precisam devolver a mesma reply se repetidos.
            if message_type == "command":
                repository.registrar_mensagem(message_id, resposta)

        pedido = dados.get("payload") if isinstance(dados.get("payload"), dict) else {}

        registrar(
            logger, "entrada", dados,
            action=pedido.get("action"),
            resultado=corpo["status"],
            code=corpo.get("error", {}).get("code"),
            alert_id=corpo.get("alert_id") or pedido.get("alert_id"),
            state=corpo.get("state"),
        )

        self.service.despachar_saida()

        return json.dumps(resposta, ensure_ascii=False)

    @staticmethod
    def _erro(codigo: str, mensagem: str) -> dict:
        return {
            "status": "ERROR",
            "error": {"code": codigo, "message": mensagem},
        }

    def _criar(self, payload: dict, message_id: str) -> dict:
        """ods.s3.alert.command"""

        for campo in ("recipients", "channels", "severity"):
            if campo not in payload:
                raise ErroS3(
                    INVALID_PAYLOAD, f"Campo obrigatório ausente: {campo}"
                )

        alerta = self.service.criar_alerta(
            titulo=_texto(payload, "title"),
            mensagem=_texto(payload, "message"),
            severidade=payload["severity"],
            destinatarios=payload["recipients"],
            canais=payload["channels"],
            evento_id=_identificador(payload, "event_id", obrigatorio=False),
            clip_id=_identificador(payload, "clip_id", obrigatorio=False),
            correlacao_id=message_id,
        )

        return {"status": "OK", "alert_id": alerta.alerta_id}

    def _acao(self, payload: dict, message_id: str) -> dict:
        """ods.s3.alert_action.command"""

        alerta_id = _identificador(payload, "alert_id")
        acao = payload.get("action")
        autor = _texto(payload, "actor")
        motivo = _texto(payload, "reason", obrigatorio=False)

        # Contrato: actor = user:<id>.
        if not autor.startswith("user:") or not autor[5:].strip():
            raise ErroS3(
                INVALID_PAYLOAD, f"actor deve ser user:<id>: {autor!r}"
            )

        # Antes de procurar o alerta: quem não pode agir não descobre
        # se o alerta existe (UNAUTHORIZED_ACTOR, extensão do VER-3).
        if self.permissoes is not None:
            self.permissoes.conferir(autor, acao)

        if acao == "ACKNOWLEDGE":
            alerta = self.service.reconhecer_alerta(alerta_id, autor, motivo)
        elif acao == "MUTE":
            alerta = self.service.silenciar_alerta(
                alerta_id,
                autor,
                _horario(payload, "mute_until"),
                motivo,
            )
        elif acao == "CLOSE":
            alerta = self.service.encerrar_alerta(alerta_id, autor, motivo)
        else:
            raise ErroS3(INVALID_PAYLOAD, f"action inválida: {acao!r}")

        return {
            "status": "OK",
            "alert_id": alerta.alerta_id,
            "state": alerta.estado,
        }

    def _consultar(self, payload: dict, message_id: str) -> dict:
        """ods.s3.alerts.query"""

        estado = payload.get("state")
        severidade = payload.get("severity")
        limite = payload.get("limit", LIMITE_PADRAO)

        if estado is not None and estado not in ESTADOS:
            raise ErroS3(INVALID_PAYLOAD, f"state inválido: {estado!r}")

        if severidade is not None and severidade not in SEVERIDADES:
            raise ErroS3(
                INVALID_PAYLOAD, f"severity inválida: {severidade!r}"
            )

        if (
            isinstance(limite, bool)
            or not isinstance(limite, int)
            or not 1 <= limite <= LIMITE_MAXIMO
        ):
            raise ErroS3(
                INVALID_PAYLOAD,
                f"limit deve estar entre 1 e {LIMITE_MAXIMO}.",
            )

        alertas, proximo = self.service.consultar_alertas(
            desde=_horario(payload, "from_at", obrigatorio=False),
            ate=_horario(payload, "to_at", obrigatorio=False),
            estado=estado,
            severidade=severidade,
            limite=limite,
        )

        corpo = {
            "status": "OK",
            "alerts": [payload_alerta(alerta) for alerta in alertas],
        }

        # Só presente quando há próxima página.
        if proximo is not None:
            corpo["next_from_at"] = formatar_horario(proximo)

        return corpo
