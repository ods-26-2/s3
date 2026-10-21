
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime
from typing import Iterator

from s3.alert import Alerta
from s3.history import RegistroHistorico

"armazenamento"
'Alertas, histórico, mensagens processadas e fila de saída'


# O S4 grava clipe de todo ods.s2.event, inclusive dos que não
# viram alerta: os pendentes mais antigos são descartados.
LIMITE_CLIPES_PENDENTES = 1000


def _ate_milissegundo(horario: datetime) -> datetime:
    """Descarta os microssegundos, como formatar_horario."""

    return horario.replace(
        microsecond=horario.microsecond // 1000 * 1000
    )


class AlertRepository:
    """
    Repositório de alertas, histórico, mensagens já
    processadas, contadores de sequence e fila de saída.

    Esta classe guarda tudo em memória (os dados se perdem
    quando o processo termina) e serve aos testes. A versão
    durável é SqliteAlertRepository, que herda as mesmas
    regras e grava cada mudança pelos métodos _ao_*.

    Devolve sempre cópias: o estado só muda por
    atualizar(), que exige um registro de histórico.

    Tudo o que muda dentro de transacao() é desfeito junto
    se algo falhar; transações podem ser aninhadas.
    """

    def __init__(self) -> None:
        self._alertas: dict[str, Alerta] = {}
        self._eventos: dict[str, str] = {}

        self._historicos: dict[
            str, list[RegistroHistorico]
        ] = {}

        # message_id → reply já enviada (None em eventos).
        self._mensagens: dict[str, dict | None] = {}

        # event_id → clip_id que chegou antes do alerta.
        self._clipes_pendentes: dict[str, str] = {}

        # session_id (ou None) → próximo sequence a usar.
        self._sequencias: dict[str | None, int] = {}

        # Fila de saída (outbox): id → mensagem a publicar.
        self._saida: dict[int, str] = {}
        self._proximo_id_saida = 1

        self._profundidade = 0

    # ------------------------------------------------------------
    # Transação

    @contextmanager
    def transacao(self) -> Iterator[None]:
        """
        Agrupa mudanças: ou todas valem, ou nenhuma. Pode ser
        aninhada; cada nível desfaz só o que fez.
        """

        self._profundidade += 1
        nivel = self._profundidade
        copia = self._capturar()
        self._abrir(nivel)

        try:
            yield
        except BaseException:
            self._desfazer(nivel, copia)
            raise
        else:
            self._confirmar(nivel)
        finally:
            self._profundidade -= 1

    def em_transacao(self) -> bool:
        return self._profundidade > 0

    def _estado(self) -> tuple:
        return (
            self._alertas,
            self._eventos,
            self._historicos,
            self._mensagens,
            self._clipes_pendentes,
            self._sequencias,
            self._saida,
            self._proximo_id_saida,
        )

    def _capturar(self) -> object:
        return deepcopy(self._estado())

    def _desfazer(self, nivel: int, copia: object) -> None:
        (
            self._alertas,
            self._eventos,
            self._historicos,
            self._mensagens,
            self._clipes_pendentes,
            self._sequencias,
            self._saida,
            self._proximo_id_saida,
        ) = copia

    def _abrir(self, nivel: int) -> None:
        pass

    def _confirmar(self, nivel: int) -> None:
        pass

    # ------------------------------------------------------------
    # Ganchos de gravação: sem efeito em memória; a versão
    # SQLite grava cada mudança.

    def _ao_salvar(self, alerta: Alerta, indexado: bool, registro: RegistroHistorico) -> None:
        pass

    def _ao_atualizar(self, alerta: Alerta, registro: RegistroHistorico | None) -> None:
        pass

    def _ao_registrar_mensagem(self, message_id: str, resposta: dict | None) -> None:
        pass

    def _ao_guardar_clipe(self, evento_id: str, clip_id: str) -> None:
        pass

    def _ao_remover_clipe(self, evento_id: str) -> None:
        pass

    def _ao_sequencia(self, session_id: str | None, proximo: int) -> None:
        pass

    def _ao_enfileirar(self, id_saida: int, mensagem: str) -> None:
        pass

    def _ao_remover_saida(self, id_saida: int) -> None:
        pass

    # ------------------------------------------------------------
    # Alertas

    def salvar(
        self,
        alerta: Alerta,
        indexar_evento: bool = True,
    ) -> None:
        """
        Salva um alerta e registra sua criação.

        indexar_evento=False para alertas criados por
        comando, que citam um event_id sem serem o
        alerta daquele evento.
        """

        alerta.validar()

        if alerta.alerta_id in self._alertas:
            raise ValueError(
                "Já existe um alerta com este ID."
            )

        if indexar_evento:
            if alerta.evento_id is None:
                raise ValueError(
                    "Um alerta de evento precisa do event_id."
                )

            if alerta.evento_id in self._eventos:
                raise ValueError(
                    "Já existe um alerta para este evento."
                )

        # Prepara o registro antes de modificar os dados.
        registro = RegistroHistorico(
            alerta_id=alerta.alerta_id,
            evento_id=alerta.evento_id,
            acao="CREATED",
            registrado_em=alerta.criado_em,
        )

        with self.transacao():
            self._alertas[alerta.alerta_id] = deepcopy(alerta)

            if indexar_evento:
                self._eventos[alerta.evento_id] = alerta.alerta_id

            self._historicos[alerta.alerta_id] = [registro]

            self._ao_salvar(deepcopy(alerta), indexar_evento, registro)

    def buscar_por_id(
        self,
        alerta_id: str
    ) -> Alerta | None:
        """
        Busca um alerta pelo ID (cópia).
        """

        alerta = self._alertas.get(alerta_id)

        return deepcopy(alerta) if alerta is not None else None

    def buscar_por_evento(
        self,
        evento_id: str
    ) -> Alerta | None:
        """
        Busca o alerta associado ao evento (cópia).
        """

        alerta_id = self._eventos.get(evento_id)

        if alerta_id is None:
            return None

        return self.buscar_por_id(alerta_id)

    def atualizar(
        self,
        alerta: Alerta,
        registro: RegistroHistorico,
    ) -> None:
        """
        Grava o novo estado do alerta junto com a ação
        que o causou. Valida tudo antes de alterar.
        """

        atual = self._alertas.get(alerta.alerta_id)

        if atual is None:
            raise ValueError(
                "Não existe alerta para este registro."
            )

        if registro.alerta_id != alerta.alerta_id:
            raise ValueError(
                "O registro não corresponde ao alerta."
            )

        if registro.evento_id != atual.evento_id:
            raise ValueError(
                "O evento do registro não corresponde ao alerta."
            )

        if registro.acao == "CREATED":
            raise ValueError(
                "A criação já é registrada automaticamente."
            )

        if registro.acao != alerta.estado:
            raise ValueError(
                "A ação do registro deve ser o novo estado."
            )

        historico = self._historicos[alerta.alerta_id]

        if any(
            item.registro_id == registro.registro_id
            for item in historico
        ):
            raise ValueError(
                "Este registro já existe no histórico."
            )

        alerta.validar()

        with self.transacao():
            self._historicos[alerta.alerta_id].append(registro)
            self._alertas[alerta.alerta_id] = deepcopy(alerta)

            self._ao_atualizar(deepcopy(alerta), registro)

    def buscar_historico(
        self,
        alerta_id: str
    ) -> tuple[RegistroHistorico, ...]:
        """
        Retorna os registros associados ao alerta.
        """

        if alerta_id not in self._alertas:
            raise ValueError(
                "Alerta não encontrado."
            )

        return tuple(
            self._historicos[alerta_id]
        )

    def listar(
        self,
        desde: datetime | None = None,
        ate: datetime | None = None,
        estado: str | None = None,
        severidade: str | None = None,
        limite: int = 100,
    ) -> tuple[list[Alerta], datetime | None]:
        """
        Alertas criados em [desde, ate), em ordem de criação.

        Retorna a página e o criado_em do próximo alerta
        (next_from_at), ou None se não houver mais.
        """

        encontrados = sorted(
            (
                alerta
                for alerta in self._alertas.values()
                if (desde is None or alerta.criado_em >= desde)
                and (ate is None or alerta.criado_em < ate)
                and (estado is None or alerta.estado == estado)
                and (
                    severidade is None
                    or alerta.severidade == severidade
                )
            ),
            # alerta_id desempata: a ordem é estável entre consultas.
            key=lambda alerta: (alerta.criado_em, alerta.alerta_id),
        )

        if len(encontrados) <= limite:
            return [deepcopy(alerta) for alerta in encontrados], None

        # next_from_at sai com milissegundos: a próxima página
        # começa no milissegundo do primeiro alerta que ficou
        # de fora. A página termina antes desse milissegundo,
        # para que a próxima não repita alertas.
        proximo = _ate_milissegundo(encontrados[limite].criado_em)

        pagina = [
            alerta
            for alerta in encontrados[:limite]
            if _ate_milissegundo(alerta.criado_em) < proximo
        ]

        if not pagina:
            # Mais de `limite` alertas no mesmo milissegundo:
            # next_from_at não consegue avançar (pendência:
            # o contrato não tem cursor além de next_from_at).
            pagina = encontrados[:limite]

        return [deepcopy(alerta) for alerta in pagina], proximo

    def definir_clipe(self, alerta_id: str, clip_id: str) -> None:
        """
        Vincula a evidência do S4 ao alerta. Não é ação humana
        nem muda o estado, por isso não entra no histórico.
        """

        alerta = self._alertas.get(alerta_id)

        if alerta is None:
            raise ValueError("Alerta não encontrado.")

        atualizado = deepcopy(alerta)
        atualizado.clip_id = clip_id
        atualizado.validar()

        with self.transacao():
            self._alertas[alerta_id] = atualizado
            self._ao_atualizar(deepcopy(atualizado), None)

    # ------------------------------------------------------------
    # Clipes que chegaram antes do alerta

    def guardar_clipe_pendente(self, evento_id: str, clip_id: str) -> None:
        """Clipe que chegou antes do alerta; vale o primeiro."""

        if evento_id in self._clipes_pendentes:
            return

        with self.transacao():
            self._clipes_pendentes[evento_id] = clip_id
            self._ao_guardar_clipe(evento_id, clip_id)

            while len(self._clipes_pendentes) > LIMITE_CLIPES_PENDENTES:
                # dict guarda a ordem de inserção: sai o mais antigo.
                antigo = next(iter(self._clipes_pendentes))
                del self._clipes_pendentes[antigo]
                self._ao_remover_clipe(antigo)

    def retirar_clipe_pendente(self, evento_id: str) -> str | None:
        if evento_id not in self._clipes_pendentes:
            return None

        with self.transacao():
            clip_id = self._clipes_pendentes.pop(evento_id)
            self._ao_remover_clipe(evento_id)

        return clip_id

    # ------------------------------------------------------------
    # Mensagens recebidas (deduplicação e idempotência)

    def mensagem_processada(self, message_id: str) -> bool:
        return message_id in self._mensagens

    def resposta_registrada(self, message_id: str) -> dict | None:
        return deepcopy(self._mensagens.get(message_id))

    def registrar_mensagem(
        self,
        message_id: str,
        resposta: dict | None = None,
    ) -> None:
        """
        Marca a mensagem como processada; em comandos,
        guarda a reply para devolvê-la se o comando
        chegar de novo (idempotência pelo message_id).
        """

        with self.transacao():
            self._mensagens[message_id] = deepcopy(resposta)
            self._ao_registrar_mensagem(message_id, deepcopy(resposta))

    # ------------------------------------------------------------
    # sequence e fila de saída (outbox)

    def proximo_sequence(self, session_id: str | None) -> int:
        """
        Próximo sequence do S3 na sessão (seção 3: contador por
        producer + session_id, a partir de 0). Sem sessão, conta
        só pelo produtor. Persistido: não volta a 0 no reinício.
        """

        with self.transacao():
            valor = self._sequencias.get(session_id, 0)
            self._sequencias[session_id] = valor + 1
            self._ao_sequencia(session_id, valor + 1)

        return valor

    def enfileirar_saida(self, mensagem: str) -> int:
        """
        Guarda uma mensagem a publicar. Chamada na mesma
        transação da mudança que a gerou: ou as duas valem,
        ou nenhuma.
        """

        with self.transacao():
            id_saida = self._proximo_id_saida
            self._proximo_id_saida += 1
            self._saida[id_saida] = mensagem
            self._ao_enfileirar(id_saida, mensagem)

        return id_saida

    def pendentes_saida(self) -> list[tuple[int, str]]:
        """Mensagens ainda não publicadas, na ordem em que entraram."""

        return sorted(self._saida.items())

    def marcar_enviada(self, id_saida: int) -> None:
        if id_saida not in self._saida:
            return

        with self.transacao():
            del self._saida[id_saida]
            self._ao_remover_saida(id_saida)
