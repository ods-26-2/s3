
import json
import sqlite3
from dataclasses import fields
from datetime import datetime

from s3.alert import Alerta
from s3.history import RegistroHistorico
from s3.repository import AlertRepository

"armazenamento durável"
'Mesmas regras do AlertRepository, gravadas em SQLite'

ESQUEMA = """
CREATE TABLE IF NOT EXISTS alertas (
    alerta_id       TEXT PRIMARY KEY,
    evento_indexado TEXT UNIQUE,          -- event_id, só em alerta de evento
    dados           TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS historico (
    ordem       INTEGER PRIMARY KEY AUTOINCREMENT,
    registro_id TEXT NOT NULL UNIQUE,
    alerta_id   TEXT NOT NULL REFERENCES alertas (alerta_id),
    dados       TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mensagens (
    message_id TEXT PRIMARY KEY,
    resposta   TEXT                       -- reply guardada (comandos)
);
CREATE TABLE IF NOT EXISTS clipes_pendentes (
    ordem     INTEGER PRIMARY KEY AUTOINCREMENT,
    evento_id TEXT NOT NULL UNIQUE,
    clip_id   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sequencias (
    chave   TEXT PRIMARY KEY,             -- session_id; '' = sem sessão
    proximo INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS saida (
    id       INTEGER PRIMARY KEY,
    mensagem TEXT NOT NULL
);
"""

# Sem sessão (alerta criado por comando): session_id é sempre UUID,
# então a chave vazia não colide.
SEM_SESSAO = ""


def _para_json(objeto: object) -> str:
    """Dataclass → JSON; datetimes em ISO 8601 com fuso."""

    dados = {}

    for campo in fields(objeto):
        valor = getattr(objeto, campo.name)
        dados[campo.name] = (
            valor.isoformat() if isinstance(valor, datetime) else valor
        )

    return json.dumps(dados, ensure_ascii=False)


def _de_json(classe: type, texto: str) -> object:
    """JSON → dataclass, convertendo de volta os datetimes."""

    dados = json.loads(texto)
    horarios = {
        campo.name
        for campo in fields(classe)
        if "datetime" in str(campo.type)
    }

    for nome in horarios:
        if dados.get(nome) is not None:
            dados[nome] = datetime.fromisoformat(dados[nome])

    return classe(**dados)


class SqliteAlertRepository(AlertRepository):
    """
    Repositório durável: alertas, histórico, mensagens
    processadas (deduplicação e replies), clipes pendentes,
    contadores de sequence e fila de saída sobrevivem ao
    reinício do processo.

    As regras são as do AlertRepository. A leitura é feita da
    memória, carregada do banco na partida; cada mudança é
    gravada na mesma transação SQLite. Se algo falhar, o banco
    volta atrás (ROLLBACK TO SAVEPOINT) e a memória é recarregada
    dele, então os dois nunca divergem.

    Um processo por arquivo: o S3 roda com um consumidor por
    processo.
    """

    def __init__(self, caminho: str = "s3.sqlite3") -> None:
        super().__init__()

        # isolation_level=None: as transações são abertas à mão,
        # com SAVEPOINT, para poderem ser aninhadas.
        self._conexao = sqlite3.connect(caminho, isolation_level=None)
        self._conexao.execute("PRAGMA foreign_keys = ON")
        self._conexao.execute("PRAGMA synchronous = FULL")

        if caminho != ":memory:":
            self._conexao.execute("PRAGMA journal_mode = WAL")

        self._conexao.executescript(ESQUEMA)
        self._carregar()

    def fechar(self) -> None:
        self._conexao.close()

    # ------------------------------------------------------------
    # Carga

    def _carregar(self) -> None:
        """Reconstrói a memória a partir do banco."""

        AlertRepository.__init__(self)
        consulta = self._conexao.execute

        for alerta_id, evento, dados in consulta(
            "SELECT alerta_id, evento_indexado, dados FROM alertas"
        ):
            self._alertas[alerta_id] = _de_json(Alerta, dados)
            self._historicos[alerta_id] = []

            if evento is not None:
                self._eventos[evento] = alerta_id

        for alerta_id, dados in consulta(
            "SELECT alerta_id, dados FROM historico ORDER BY ordem"
        ):
            self._historicos[alerta_id].append(
                _de_json(RegistroHistorico, dados)
            )

        for message_id, resposta in consulta(
            "SELECT message_id, resposta FROM mensagens"
        ):
            self._mensagens[message_id] = (
                json.loads(resposta) if resposta is not None else None
            )

        for evento_id, clip_id in consulta(
            "SELECT evento_id, clip_id FROM clipes_pendentes ORDER BY ordem"
        ):
            self._clipes_pendentes[evento_id] = clip_id

        for chave, proximo in consulta(
            "SELECT chave, proximo FROM sequencias"
        ):
            self._sequencias[chave if chave != SEM_SESSAO else None] = proximo

        for id_saida, mensagem in consulta(
            "SELECT id, mensagem FROM saida ORDER BY id"
        ):
            self._saida[id_saida] = mensagem

        (maior,) = consulta("SELECT COALESCE(MAX(id), 0) FROM saida").fetchone()
        self._proximo_id_saida = max(maior + 1, self._proximo_id_saida)

    # ------------------------------------------------------------
    # Transação: um SAVEPOINT por nível

    def _capturar(self) -> object:
        # Não copia a memória: em caso de falha, ela é recarregada.
        return None

    def _abrir(self, nivel: int) -> None:
        self._conexao.execute(f"SAVEPOINT nivel_{nivel}")

    def _confirmar(self, nivel: int) -> None:
        # RELEASE do nível 1 grava em disco (COMMIT).
        self._conexao.execute(f"RELEASE nivel_{nivel}")

    def _desfazer(self, nivel: int, copia: object) -> None:
        self._conexao.execute(f"ROLLBACK TO nivel_{nivel}")
        self._conexao.execute(f"RELEASE nivel_{nivel}")

        # A profundidade ainda conta este nível; a carga recria o
        # estado e precisa preservá-la.
        profundidade = self._profundidade
        self._carregar()
        self._profundidade = profundidade

    # ------------------------------------------------------------
    # Gravação

    def _ao_salvar(self, alerta: Alerta, indexado: bool, registro: RegistroHistorico) -> None:
        self._conexao.execute(
            "INSERT INTO alertas (alerta_id, evento_indexado, dados) VALUES (?, ?, ?)",
            (alerta.alerta_id, alerta.evento_id if indexado else None, _para_json(alerta)),
        )
        self._gravar_registro(registro)

    def _ao_atualizar(self, alerta: Alerta, registro: RegistroHistorico | None) -> None:
        self._conexao.execute(
            "UPDATE alertas SET dados = ? WHERE alerta_id = ?",
            (_para_json(alerta), alerta.alerta_id),
        )

        if registro is not None:
            self._gravar_registro(registro)

    def _gravar_registro(self, registro: RegistroHistorico) -> None:
        self._conexao.execute(
            "INSERT INTO historico (registro_id, alerta_id, dados) VALUES (?, ?, ?)",
            (registro.registro_id, registro.alerta_id, _para_json(registro)),
        )

    def _ao_registrar_mensagem(self, message_id: str, resposta: dict | None) -> None:
        self._conexao.execute(
            "INSERT OR REPLACE INTO mensagens (message_id, resposta) VALUES (?, ?)",
            (
                message_id,
                json.dumps(resposta, ensure_ascii=False) if resposta is not None else None,
            ),
        )

    def _ao_guardar_clipe(self, evento_id: str, clip_id: str) -> None:
        self._conexao.execute(
            "INSERT INTO clipes_pendentes (evento_id, clip_id) VALUES (?, ?)",
            (evento_id, clip_id),
        )

    def _ao_remover_clipe(self, evento_id: str) -> None:
        self._conexao.execute(
            "DELETE FROM clipes_pendentes WHERE evento_id = ?", (evento_id,)
        )

    def _ao_sequencia(self, session_id: str | None, proximo: int) -> None:
        self._conexao.execute(
            "INSERT OR REPLACE INTO sequencias (chave, proximo) VALUES (?, ?)",
            (session_id if session_id is not None else SEM_SESSAO, proximo),
        )

    def _ao_enfileirar(self, id_saida: int, mensagem: str) -> None:
        self._conexao.execute(
            "INSERT INTO saida (id, mensagem) VALUES (?, ?)", (id_saida, mensagem)
        )

    def _ao_remover_saida(self, id_saida: int) -> None:
        self._conexao.execute("DELETE FROM saida WHERE id = ?", (id_saida,))
