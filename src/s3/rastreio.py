import logging

"rastreabilidade"
'Uma linha de log por mensagem que entra ou sai do S3, em chave=valor'

# Campos do envelope que identificam a mensagem e a ligam às outras:
# message_id (a própria), correlation_id (a que a causou) e, nos
# eventos, a sessão e a ordem.
CAMPOS_ENVELOPE = ("schema", "message_id", "producer", "correlation_id", "session_id", "sequence")


def _valor(valor: object) -> str:
    texto = str(valor)
    return f'"{texto}"' if " " in texto else texto


def linha(direcao: str, mensagem: dict | None, **extras: object) -> str:
    """
    direcao=campo=valor ... Campos sem valor são omitidos, para
    a linha ficar curta e fácil de procurar (grep message_id=...).
    """

    partes = [direcao]
    envelope = mensagem or {}

    for campo in CAMPOS_ENVELOPE:
        if envelope.get(campo) is not None:
            partes.append(f"{campo}={_valor(envelope[campo])}")

    for campo, valor in extras.items():
        if valor is not None:
            partes.append(f"{campo}={_valor(valor)}")

    return " ".join(partes)


def registrar(logger: logging.Logger, direcao: str, mensagem: dict | None, **extras: object) -> None:
    """Registra em INFO uma mensagem recebida ("entrada") ou publicada ("saida")."""

    logger.info(linha(direcao, mensagem, **extras))
