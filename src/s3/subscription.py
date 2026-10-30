
import json
import logging
from typing import Callable, Protocol

from s3.alert import Alerta
from s3.commands import ProcessadorComandos
from s3.envelope import decodificar
from s3.ingestion import (
    SCHEMA_CLIPE,
    SCHEMA_ENTRADA,
    IngestorClipes,
    IngestorEventos,
)
from s3.publication import PublicadorAlertas
from s3.rastreio import linha, registrar

"fronteira de assinatura com o barramento de eventos (B3)"
"o adaptador Kafka fica em kafka_adapter.py; aqui só o contrato interno"

logger = logging.getLogger(__name__)


def _envelope(mensagem: str | bytes) -> dict | None:
    """O envelope da mensagem, se ela for JSON; senão None."""

    try:
        return decodificar(mensagem)
    except ValueError:
        return None


def _descartar(mensagem: str | bytes, erro: Exception) -> None:
    logger.warning(linha("descartada", _envelope(mensagem), motivo=str(erro)))


class AssinanteEventos(Protocol):
    """
    Contrato que um adaptador de barramento deve cumprir
    para entregar mensagens ao S3 (texto ou bytes).
    """

    def assinar(self, callback: Callable[[str | bytes], object]) -> None:
        ...


class ConectorEventos:
    """
    Liga um assinante de ods.s2.event (ou de ods.s4.clip)
    ao ingestor correspondente do S3.

    Mensagem inválida é descartada e registrada em log,
    nunca processada "na tentativa" (Guia do Kafka, 4.3).
    """

    def __init__(
        self,
        assinante: AssinanteEventos,
        ingestor: IngestorEventos | IngestorClipes,
    ) -> None:

        self.assinante = assinante
        self.ingestor = ingestor

    def conectar(self) -> None:
        """
        Registra o conector como callback do assinante.
        """

        self.assinante.assinar(self.entregar)

    def entregar(self, mensagem: str | bytes) -> Alerta | None:
        try:
            return self.ingestor.receber(mensagem)
        except ValueError as erro:
            _descartar(mensagem, erro)
            return None


class ConectorComandos:
    """
    Liga um assinante dos comandos do S3 ao processador
    e publica a reply de cada comando.
    """

    def __init__(
        self,
        assinante: AssinanteEventos,
        processador: ProcessadorComandos,
        publicador_respostas: PublicadorAlertas,
    ) -> None:

        self.assinante = assinante
        self.processador = processador
        self.publicador_respostas = publicador_respostas

    def conectar(self) -> None:
        self.assinante.assinar(self.entregar)

    def entregar(self, mensagem: str | bytes) -> str | None:
        try:
            resposta = self.processador.receber(mensagem)
        except ValueError as erro:
            _descartar(mensagem, erro)
            return None

        self.publicador_respostas.publicar(resposta)

        dados = json.loads(resposta)
        registrar(
            logger, "saida", dados,
            status=dados["payload"].get("status"),
            code=dados["payload"].get("error", {}).get("code"),
        )

        return resposta


class DespachanteS3:
    """
    Um único assinante para todos os tópicos do S3:
    envia ods.s2.event ao ingestor de eventos, ods.s4.clip
    ao de clipes e o resto aos comandos.
    """

    def __init__(
        self,
        eventos: ConectorEventos,
        comandos: ConectorComandos,
        clipes: ConectorEventos | None = None,
    ) -> None:

        self.eventos = eventos
        self.comandos = comandos

        # Mesmo conector dos eventos: descarta inválidos com log.
        self.clipes = clipes

    def conectar(self, assinante: AssinanteEventos) -> None:
        assinante.assinar(self.entregar)

    def entregar(self, mensagem: str | bytes) -> object:
        try:
            schema = decodificar(mensagem).get("schema")
        except ValueError as erro:
            _descartar(mensagem, erro)
            return None

        if schema == SCHEMA_ENTRADA:
            return self.eventos.entregar(mensagem)

        if schema == SCHEMA_CLIPE:
            if self.clipes is None:
                logger.warning("ods.s4.clip sem conector; descartado")
                return None

            return self.clipes.entregar(mensagem)

        return self.comandos.entregar(mensagem)
