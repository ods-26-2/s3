"""
Verificador de contrato escrito a partir da Especificação de
interfaces 1.0 (seções 2, 3 e 7), sem usar o código do S3.

Serve para conferir as mensagens que o S3 publica com regras
independentes das validações internas: se as duas concordarem
por engano, este arquivo ainda acusa. Não substitui o validador
de B1, que ainda não foi publicado.

Uma regra não é da especificação: session_id opcional em
ods.s3.alert criado por comando (proposta do VER-3, pendência 5,
aguardando B1/B2). Ela só vale com origem="comando".
"""

import re

UUID_V4 = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
    re.IGNORECASE,
)

# Seção 2, regra 2: ISO 8601 em UTC com milissegundos.
HORARIO = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")

CODIGO_ERRO = re.compile(r"[A-Z][A-Z0-9]*(_[A-Z0-9]+)*")

# Seção 3: os 13 campos do envelope, mais o payload.
FIXOS = (
    "message_type", "message_id", "schema",
    "schema_version", "producer", "published_at",
)
CONTEXTO = (
    "target", "correlation_id", "captured_at", "source_id",
    "session_id", "sequence", "artifact_version",
)
SINAL = ("captured_at", "source_id", "session_id", "sequence")

# Seção 7, ods.s3.alert.
ALERTA_OBRIGATORIOS = (
    "alert_id", "title", "message", "severity",
    "recipients", "channels", "state", "created_at",
)
ALERTA_OPCIONAIS = (
    "event_id", "clip_id", "acknowledged_by", "acknowledged_at",
    "muted_until", "closed_by", "closed_at",
)


class ViolacaoContrato(AssertionError):
    pass


def _exigir(condicao: bool, mensagem: str) -> None:
    if not condicao:
        raise ViolacaoContrato(mensagem)


def _uuid(valor, campo):
    _exigir(
        isinstance(valor, str) and UUID_V4.fullmatch(valor) is not None,
        f"{campo} não é UUID v4: {valor!r}",
    )


def _horario(valor, campo):
    _exigir(
        isinstance(valor, str) and HORARIO.fullmatch(valor) is not None,
        f"{campo} não é ISO 8601 UTC com milissegundos: {valor!r}",
    )


def _texto(valor, campo):
    _exigir(
        isinstance(valor, str) and valor.strip() != "",
        f"{campo} deve ser texto não vazio: {valor!r}",
    )


def envelope(
    mensagem: dict,
    perfil: str,
    sessao_opcional: bool = False,
) -> dict:
    """
    Confere o envelope pelo perfil (EV-SERVIÇO, CMD ou RPL)
    e devolve o payload.

    sessao_opcional: regra proposta para alerta criado por
    comando (EV-SERVIÇO sem session_id).
    """

    _exigir(isinstance(mensagem, dict), "a mensagem deve ser um objeto")

    desconhecidos = set(mensagem) - set(FIXOS) - set(CONTEXTO) - {"payload"}
    _exigir(not desconhecidos, f"campos fora do envelope: {desconhecidos}")

    for campo in FIXOS + ("payload",):
        _exigir(campo in mensagem, f"campo fixo ausente: {campo}")

    _exigir(
        mensagem["message_type"] in ("event", "command", "query", "reply"),
        f"message_type inválido: {mensagem['message_type']!r}",
    )
    _uuid(mensagem["message_id"], "message_id")
    _exigir(
        re.fullmatch(r"ods\.[a-z0-9_]+(\.[a-z0-9_]+)+", mensagem["schema"]) is not None,
        f"schema fora da regra 1: {mensagem['schema']!r}",
    )
    _exigir(
        isinstance(mensagem["schema_version"], str)
        and re.fullmatch(r"\d+\.\d+", mensagem["schema_version"]) is not None,
        f"schema_version não é MAJOR.MINOR: {mensagem['schema_version']!r}",
    )
    _texto(mensagem["producer"], "producer")
    _horario(mensagem["published_at"], "published_at")
    _exigir(isinstance(mensagem["payload"], dict), "payload deve ser objeto")

    # Seção 3: o payload nunca repete um campo do envelope.
    repetidos = set(mensagem["payload"]) & (set(FIXOS) | set(CONTEXTO))
    _exigir(not repetidos, f"payload repete o envelope: {repetidos}")

    if perfil == "EV-SERVIÇO":
        _exigir(mensagem["message_type"] == "event", "EV-SERVIÇO é event")

        obrigatorios = ("captured_at", "sequence")

        if not sessao_opcional:
            obrigatorios += ("session_id",)

        for campo in obrigatorios:
            _exigir(campo in mensagem, f"EV-SERVIÇO exige {campo}")

        _horario(mensagem["captured_at"], "captured_at")

        if "session_id" in mensagem:
            _uuid(mensagem["session_id"], "session_id")
        else:
            # Sem sessão, a origem precisa ficar rastreável.
            _exigir(
                "correlation_id" in mensagem,
                "sem session_id, exige correlation_id do comando",
            )
        _exigir(
            isinstance(mensagem["sequence"], int)
            and not isinstance(mensagem["sequence"], bool)
            and mensagem["sequence"] >= 0,
            f"sequence inválido: {mensagem['sequence']!r}",
        )

        if "correlation_id" in mensagem:
            _uuid(mensagem["correlation_id"], "correlation_id")

    elif perfil == "CMD":
        _exigir(
            mensagem["message_type"] in ("command", "query"),
            "CMD é command ou query",
        )
        _texto(mensagem.get("target"), "target")

        for campo in SINAL:
            _exigir(campo not in mensagem, f"CMD sem campos de sinal: {campo}")

    elif perfil == "RPL":
        _exigir(mensagem["message_type"] == "reply", "RPL é reply")
        _exigir(
            mensagem["schema"].endswith(".reply"),
            "reply termina em .reply",
        )
        _uuid(mensagem.get("correlation_id"), "correlation_id")

        for campo in SINAL + ("target",):
            _exigir(campo not in mensagem, f"RPL sem {campo}")

        reply(mensagem["payload"])

    else:
        raise ValueError(f"perfil desconhecido: {perfil}")

    return mensagem["payload"]


def reply(payload: dict) -> None:
    """Seção 2, regra 6: status e, em erro, error {code, message}."""

    _exigir(payload.get("status") in ("OK", "ERROR"), "status OK ou ERROR")

    if payload["status"] == "ERROR":
        erro = payload.get("error")
        _exigir(isinstance(erro, dict), "ERROR exige error")
        _exigir(
            isinstance(erro.get("code"), str)
            and CODIGO_ERRO.fullmatch(erro["code"]) is not None,
            f"code não é UPPER_SNAKE_CASE: {erro.get('code')!r}",
        )
        _texto(erro.get("message"), "error.message")
    else:
        _exigir("error" not in payload, "OK não traz error")


def payload_alerta(payload: dict) -> None:
    """Tabela de campos de ods.s3.alert (seção 7)."""

    desconhecidos = set(payload) - set(ALERTA_OBRIGATORIOS) - set(ALERTA_OPCIONAIS)
    _exigir(not desconhecidos, f"campos fora de ods.s3.alert: {desconhecidos}")

    for campo in ALERTA_OBRIGATORIOS:
        _exigir(campo in payload, f"ods.s3.alert sem {campo}")

    _uuid(payload["alert_id"], "alert_id")
    _texto(payload["title"], "title")
    _texto(payload["message"], "message")
    _exigir(
        payload["severity"] in ("HIGH", "MEDIUM", "LOW"),
        f"severity inválida: {payload['severity']!r}",
    )
    _exigir(
        isinstance(payload["recipients"], list) and payload["recipients"],
        "recipients deve ser lista não vazia",
    )

    for destinatario in payload["recipients"]:
        _exigir(
            isinstance(destinatario, str)
            and re.fullmatch(r"(user|group|role):\S.*", destinatario) is not None,
            f"recipient inválido: {destinatario!r}",
        )

    _exigir(
        isinstance(payload["channels"], list) and payload["channels"],
        "channels deve ser lista não vazia",
    )

    for canal in payload["channels"]:
        _exigir(
            canal in ("SOUND", "SCREEN", "PUSH"),
            f"channel inválido: {canal!r}",
        )

    _exigir(
        payload["state"] in ("CREATED", "ACKNOWLEDGED", "MUTED", "CLOSED"),
        f"state inválido: {payload['state']!r}",
    )
    _horario(payload["created_at"], "created_at")

    for campo in ("event_id", "clip_id"):
        if campo in payload:
            _uuid(payload[campo], campo)

    for campo in ("acknowledged_by", "closed_by"):
        if campo in payload:
            _texto(payload[campo], campo)

    for campo in ("acknowledged_at", "muted_until", "closed_at"):
        if campo in payload:
            _horario(payload[campo], campo)


def mensagem_alerta(mensagem: dict, origem: str = "evento") -> dict:
    """
    ods.s3.alert completo: envelope EV-SERVIÇO e payload.

    origem="evento": alerta de ods.s2.event; session_id obrigatório
    e correlation_id = event_id (seção 7).
    origem="comando": alerta de ods.s3.alert.command; session_id
    opcional (regra proposta, pendência 5).
    """

    if origem not in ("evento", "comando"):
        raise ValueError(f"origem desconhecida: {origem}")

    payload = envelope(
        mensagem, "EV-SERVIÇO", sessao_opcional=origem == "comando"
    )

    _exigir(mensagem["schema"] == "ods.s3.alert", "schema ods.s3.alert")
    _exigir(mensagem["producer"] == "S3", "produtor S3")
    payload_alerta(payload)

    if origem == "evento":
        _exigir(
            mensagem.get("correlation_id") == payload.get("event_id"),
            "alerta de evento: correlation_id deve ser o event_id",
        )

    return payload
