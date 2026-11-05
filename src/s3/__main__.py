"""
Executa o S3 ligado ao Kafka.

    PowerShell:
        $env:PYTHONPATH = "src"
        $env:S3_KAFKA_BOOTSTRAP = "host:porta"
        python -m s3 --routing contratos/Barramentos/ROUTING.json --tempo-limite 60

Sem --routing (build sem S2, como a do A7), o S3 só atende
comandos e consultas.

Alertas, histórico, deduplicação e a fila de saída ficam no
SQLite de --banco (padrão: s3.sqlite3). Mensagens que não foram
publicadas antes de uma queda saem na partida seguinte.

O broker de testes é pago e compartilhado: use sempre
--tempo-limite ou --max-mensagens (Guia do Kafka, 4.5).
"""

import argparse
import json
import logging
import os
import signal

from s3.commands import ProcessadorComandos
from s3.ingestion import IngestorClipes, IngestorEventos
from s3.kafka_adapter import (
    TOPICOS_SAIDA,
    AssinanteKafka,
    PublicadorKafka,
    garantir_topicos,
    topicos_entrada,
)
from s3.permissoes import PoliticaPermissoes
from s3.repository_sqlite import SqliteAlertRepository
from s3.routing import RoteadorAlertas
from s3.service import AlertService
from s3.subscription import (
    ConectorComandos,
    ConectorEventos,
    DespachanteS3,
)


VARIAVEL_PRODUTORES = "S3_PRODUTORES_COMANDOS"


def main() -> None:
    parser = argparse.ArgumentParser(description="S3 — Alertas e notificações")
    parser.add_argument(
        "--routing",
        help=(
            "Arquivo JSON com o conteúdo do artefato ROUTING. Sem ele, "
            "o S3 só atende comandos (build sem S2, como a do A7)"
        ),
    )
    parser.add_argument(
        "--versao-routing",
        help="artifact_version do ROUTING (padrão: nome do arquivo)",
    )
    parser.add_argument(
        "--permissoes",
        help=(
            "Arquivo JSON com quem pode dar ACKNOWLEDGE, MUTE e CLOSE. "
            "Sem ele, qualquer actor user:<id> pode agir"
        ),
    )
    parser.add_argument(
        "--banco", default="s3.sqlite3",
        help="Arquivo SQLite com alertas, histórico e fila de saída",
    )
    parser.add_argument(
        "--produtores",
        default=os.environ.get(VARIAVEL_PRODUTORES, ""),
        help=(
            "Quem pode mandar comandos e consultas ao S3, separado por "
            "vírgula (ex.: A3,A5,A7). Padrão: a variável "
            f"{VARIAVEL_PRODUTORES}; vazio = qualquer produtor"
        ),
    )
    parser.add_argument(
        "--criar-topicos", action="store_true",
        help=(
            "Na partida, cria no broker os tópicos do S3 que faltarem "
            "(a imagem Docker liga; a TRA-1 liberou em 05/11/2026)"
        ),
    )
    parser.add_argument("--max-mensagens", type=int)
    parser.add_argument("--tempo-limite", type=float)
    argumentos = parser.parse_args()

    if argumentos.max_mensagens is None and argumentos.tempo_limite is None:
        parser.error("Informe --max-mensagens ou --tempo-limite.")

    produtores = frozenset(
        p.strip() for p in argumentos.produtores.split(",") if p.strip()
    ) or None

    logging.basicConfig(level=logging.INFO)

    roteador = None

    if argumentos.routing:
        with open(argumentos.routing, encoding="utf-8") as arquivo:
            conteudo = json.load(arquivo)

        versao = argumentos.versao_routing or argumentos.routing
        roteador = RoteadorAlertas.de_artefato(conteudo, versao)
    else:
        logging.info("sem ROUTING: só comandos e consultas")

    permissoes = None

    if argumentos.permissoes:
        with open(argumentos.permissoes, encoding="utf-8") as arquivo:
            permissoes = PoliticaPermissoes.de_artefato(
                json.load(arquivo), argumentos.permissoes
            )
    else:
        logging.warning(
            "sem --permissoes: qualquer actor user:<id> pode reconhecer, silenciar e encerrar"
        )

    if produtores:
        logging.info("comandos aceitos só de: %s", ", ".join(sorted(produtores)))
    else:
        logging.warning(
            "sem --produtores: qualquer produtor pode mandar comandos ao S3 "
            "(o broker não tem autenticação)"
        )

    repository = SqliteAlertRepository(argumentos.banco)
    publicador = PublicadorKafka()
    service = AlertService(
        repository=repository, roteador=roteador, publicador=publicador
    )

    # O que ficou na fila de saída antes de uma queda sai agora.
    reenviadas = service.despachar_saida()

    if reenviadas:
        logging.info("fila de saída: %d mensagem(ns) reenviada(s)", reenviadas)

    topicos = topicos_entrada(roteador is not None)

    if argumentos.criar_topicos:
        # Antes de assinar: tópico criado depois pode levar até 5 min
        # para ser percebido. Falhar aqui não impede o S3 de ligar.
        try:
            criados = garantir_topicos(topicos=topicos + TOPICOS_SAIDA)
            logging.info(
                "tópicos do S3: %s",
                f"criados {', '.join(criados)}" if criados else "todos já existiam",
            )
        except Exception as erro:
            logging.warning("não foi possível conferir os tópicos no broker: %s", erro)

    assinante = AssinanteKafka(topicos=topicos)
    DespachanteS3(
        ConectorEventos(assinante, IngestorEventos(service)),
        ConectorComandos(assinante, ProcessadorComandos(service, permissoes, produtores), publicador),
        ConectorEventos(assinante, IngestorClipes(service)),
    ).conectar(assinante)

    def ao_sinal(numero, _quadro):
        # `docker stop` e `systemctl stop` mandam SIGTERM: o laço termina a
        # volta atual e sai, fechando o consumidor (o Kafka libera o lugar
        # no grupo na hora) e o banco, como no Ctrl+C.
        logging.info("S3 recebeu %s; encerrando", signal.Signals(numero).name)
        assinante.parar()

    signal.signal(signal.SIGTERM, ao_sinal)

    try:
        entregues = assinante.executar(
            max_mensagens=argumentos.max_mensagens,
            tempo_limite_s=argumentos.tempo_limite,
            # Reenvia a fila de saída quando o broker volta.
            periodica=service.despachar_saida,
        )
    except KeyboardInterrupt:
        # Ctrl+C: o assinante já fechou o consumidor (saiu do grupo) e o
        # banco fecha abaixo. O que não foi confirmado volta na próxima partida.
        logging.info("S3 parado com Ctrl+C")
        return
    finally:
        repository.fechar()

    logging.info("mensagens processadas: %d", entregues)


if __name__ == "__main__":
    main()
