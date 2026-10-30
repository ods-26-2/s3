from dataclasses import dataclass

from s3.errors import INVALID_PAYLOAD, UNAUTHORIZED_ACTOR, ErroS3

"permissões de ação"
'Quem pode dar ACKNOWLEDGE, MUTE e CLOSE em cada build (Sprint 3, S3.4)'

ACOES = ("ACKNOWLEDGE", "MUTE", "CLOSE")
QUALQUER_UM = "*"
PREFIXOS_GRUPO = ("role:", "group:")


def _texto_com_prefixo(valor: object, prefixos: tuple[str, ...]) -> bool:
    return isinstance(valor, str) and any(
        valor.startswith(prefixo) and valor[len(prefixo):].strip()
        for prefixo in prefixos
    )


@dataclass(frozen=True)
class PoliticaPermissoes:
    """
    Arquivo de configuração da build, escrito pela aplicação
    (extensão do VER-3: a especificação não define quem pode agir).

        {
          "members": { "role:supervisor": ["user:maria", "user:joao"] },
          "actions": {
            "ACKNOWLEDGE": ["role:supervisor", "user:ana"],
            "MUTE": ["role:supervisor"],
            "CLOSE": ["role:supervisor"]
          }
        }

    Em "actions", cada item é "*" (qualquer actor), "user:<id>" ou um
    "role:<id>" / "group:<id>" declarado em "members". Ação que não
    aparece em "actions" é negada a todos.
    """

    membros: dict[str, frozenset[str]]
    acoes: dict[str, frozenset[str]]
    versao: str | None = None

    @classmethod
    def de_artefato(cls, conteudo: object, versao: str | None = None) -> "PoliticaPermissoes":
        """Valida o arquivo; um erro aqui impede o S3 de subir."""

        if not isinstance(conteudo, dict):
            raise ValueError("O arquivo de permissões deve ser um objeto.")

        membros_brutos = conteudo.get("members", {})
        acoes_brutas = conteudo.get("actions")

        if not isinstance(membros_brutos, dict):
            raise ValueError("members deve ser um objeto.")

        if not isinstance(acoes_brutas, dict) or not acoes_brutas:
            raise ValueError("O arquivo de permissões precisa de actions.")

        membros: dict[str, frozenset[str]] = {}

        for grupo, usuarios in membros_brutos.items():
            if not _texto_com_prefixo(grupo, PREFIXOS_GRUPO):
                raise ValueError(f"members: {grupo!r} deve ser role:<id> ou group:<id>.")

            if not isinstance(usuarios, list) or not all(
                _texto_com_prefixo(u, ("user:",)) for u in usuarios
            ):
                raise ValueError(f"members[{grupo!r}] deve ser uma lista de user:<id>.")

            membros[grupo] = frozenset(usuarios)

        acoes: dict[str, frozenset[str]] = {}

        for acao, quem in acoes_brutas.items():
            if acao not in ACOES:
                raise ValueError(f"actions: ação desconhecida {acao!r}.")

            if not isinstance(quem, list) or not quem:
                raise ValueError(f"actions[{acao!r}] deve ser uma lista não vazia.")

            for item in quem:
                if item == QUALQUER_UM or _texto_com_prefixo(item, ("user:",)):
                    continue

                if _texto_com_prefixo(item, PREFIXOS_GRUPO):
                    # Erro de digitação num papel negaria a ação sem aviso.
                    if item not in membros:
                        raise ValueError(f"actions[{acao!r}]: {item!r} não está em members.")
                    continue

                raise ValueError(
                    f"actions[{acao!r}]: {item!r} deve ser *, user:<id>, role:<id> ou group:<id>."
                )

            acoes[acao] = frozenset(quem)

        return cls(membros=membros, acoes=acoes, versao=versao)

    def permite(self, actor: str, acao: str) -> bool:
        permitidos = self.acoes.get(acao)

        if not permitidos:
            return False

        if QUALQUER_UM in permitidos or actor in permitidos:
            return True

        return any(
            actor in self.membros.get(grupo, ())
            for grupo in permitidos
            if grupo.startswith(PREFIXOS_GRUPO)
        )

    def conferir(self, actor: str, acao: str) -> None:
        """ErroS3 UNAUTHORIZED_ACTOR quando o actor não pode executar a ação."""

        if acao not in ACOES:
            raise ErroS3(INVALID_PAYLOAD, f"action inválida: {acao!r}")

        if not self.permite(actor, acao):
            raise ErroS3(UNAUTHORIZED_ACTOR, f"{actor} não tem permissão para {acao}.")
