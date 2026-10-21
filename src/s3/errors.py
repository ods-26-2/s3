
"erros com código"
'O código vai no campo error.code das replies (Especificação 1.0, seção 2, regra 6)'

# Códigos definidos na Especificação de interfaces 1.0 para o S3.
ALERT_NOT_FOUND = "ALERT_NOT_FOUND"
INVALID_TRANSITION = "INVALID_TRANSITION"
INVALID_RECIPIENT = "INVALID_RECIPIENT"
INVALID_CHANNEL = "INVALID_CHANNEL"

# Códigos do S3 definidos pelo VER-3 (dono da tabela de erros do S3,
# seção 2, regra 6), ainda não escritos na especificação; avisar o
# TRA-1 na fase de testes (pendência 9).
INVALID_MESSAGE = "INVALID_MESSAGE"
INVALID_PAYLOAD = "INVALID_PAYLOAD"


class ErroS3(ValueError):
    """
    Erro de validação ou de regra do S3, com código.

    É um ValueError, para que quem só quer saber se a
    mensagem foi rejeitada não precise conhecer os códigos.
    """

    def __init__(self, codigo: str, mensagem: str) -> None:
        super().__init__(mensagem)
        self.codigo = codigo
