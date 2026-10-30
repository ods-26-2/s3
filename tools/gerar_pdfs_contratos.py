"""
Gera os PDFs de contrato do S3 para as equipes:

    contratos/S2/CONTRATO_S3_PARA_S2.pdf
    contratos/Aplicações/CONTRATOS_S3_PARA_APLICACOES.pdf
    contratos/Barramentos/CONTRATOS_S3_PARA_BARRAMENTO.pdf

Os exemplos de mensagem são produzidos rodando o próprio S3 (src/s3),
a partir dos exemplos oficiais de tests/s3/dados, para não divergirem
do código. A versão (branch e commit) sai do git.

Uso, na raiz do repositório (Windows: usa as fontes Calibri, Consolas
e Segoe UI Symbol do sistema):

    pip install reportlab          # só para este script; não é dependência do S3
    python tools/gerar_pdfs_contratos.py
"""

import datetime
import json
import os
import subprocess
import sys
from uuid import uuid4

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAIDA = os.path.join(RAIZ, "contratos")
sys.path[:0] = [os.path.join(RAIZ, "src")]

from reportlab.lib import colors  # noqa: E402
from reportlab.lib.pagesizes import A4  # noqa: E402
from reportlab.lib.styles import ParagraphStyle  # noqa: E402
from reportlab.lib.units import cm  # noqa: E402
from reportlab.pdfbase import pdfmetrics  # noqa: E402
from reportlab.pdfbase.ttfonts import TTFont  # noqa: E402
from reportlab.platypus import (  # noqa: E402
    KeepTogether, Paragraph, Preformatted, SimpleDocTemplate, Spacer, Table, TableStyle,
)

from s3.commands import ProcessadorComandos  # noqa: E402
from s3.ingestion import IngestorEventos  # noqa: E402
from s3.routing import RoteadorAlertas  # noqa: E402
from s3.service import AlertService  # noqa: E402


def _git(*argumentos):
    try:
        return subprocess.run(
            ["git", *argumentos], cwd=RAIZ, capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "?"


VERSAO = (
    f"S3 na branch {_git('branch', '--show-current')}, commit {_git('rev-parse', '--short', 'HEAD')} · "
    f"{datetime.date.today():%d/%m/%Y}"
)

# ------------------------------------------------------------------
# Exemplos reais, gerados pelo S3


def ler(nome):
    with open(os.path.join(RAIZ, "tests", "s3", "dados", nome), encoding="utf-8") as arquivo:
        return json.load(arquivo)


class Publicador:
    def __init__(self):
        self.mensagens = []

    def publicar(self, mensagem):
        self.mensagens.append(json.loads(mensagem))


publicador = Publicador()
service = AlertService(
    roteador=RoteadorAlertas.de_artefato(ler("routing_a5.json"), "routing-a5-1"),
    publicador=publicador,
)
processador = ProcessadorComandos(service)
IngestorEventos(service).receber(json.dumps(ler("entrada.json")))
ALERTA_EVENTO = publicador.mensagens[-1]
ALERT_ID = ALERTA_EVENTO["payload"]["alert_id"]


def enviar(dados):
    return json.loads(processador.receber(json.dumps(dados)))


ACAO = ler("comando_acao.json")
ACAO["payload"]["alert_id"] = ALERT_ID
ACAO["payload"]["action"] = "ACKNOWLEDGE"
del ACAO["payload"]["mute_until"]
ACAO["payload"]["reason"] = "supervisora a caminho"
REPLY_ACAO = enviar(ACAO)

ACAO_ERRO = dict(ACAO, message_id=str(uuid4()))
REPLY_ERRO = enviar(ACAO_ERRO)

CRIAR = ler("comando_criar.json")
REPLY_CRIAR = enviar(CRIAR)
ALERTA_COMANDO = publicador.mensagens[-1]

CONSULTA = ler("consulta_alertas.json")
CONSULTA["payload"] = {"severity": "HIGH", "limit": 1}
REPLY_CONSULTA = enviar(CONSULTA)


def js(dados):
    return json.dumps(dados, ensure_ascii=False, indent=2)


# ------------------------------------------------------------------
# Estilo

pdfmetrics.registerFont(TTFont("Calibri", "C:/Windows/Fonts/calibri.ttf"))
pdfmetrics.registerFont(TTFont("Calibri-Bold", "C:/Windows/Fonts/calibrib.ttf"))
pdfmetrics.registerFont(TTFont("Consolas", "C:/Windows/Fonts/consola.ttf"))
pdfmetrics.registerFont(TTFont("Simbolos", "C:/Windows/Fonts/seguisym.ttf"))
pdfmetrics.registerFontFamily("Calibri", normal="Calibri", bold="Calibri-Bold")

AZUL = colors.HexColor("#15324e")
CINZA = colors.HexColor("#4a5666")
LINHA = colors.HexColor("#d9dee5")
FUNDO = colors.HexColor("#eef3f9")
CODIGO = colors.HexColor("#f6f8fa")
MARCAS = {
    "Oficial": colors.HexColor("#1f4f7a"),
    "Aprovada": colors.HexColor("#2e7d4f"),
    "Do VER-3": colors.HexColor("#6b3fa0"),
    "Provisória": colors.HexColor("#b26b00"),
}

base = ParagraphStyle("base", fontName="Calibri", fontSize=10, leading=13.5)
titulo = ParagraphStyle("t", parent=base, fontName="Calibri-Bold", fontSize=19, leading=23, textColor=AZUL)
sub = ParagraphStyle("s", parent=base, fontSize=10.5, textColor=CINZA)
h2 = ParagraphStyle("h2", parent=base, fontName="Calibri-Bold", fontSize=14, leading=18, textColor=AZUL,
                    spaceBefore=12, spaceAfter=4, keepWithNext=1)
h3 = ParagraphStyle("h3", parent=base, fontName="Calibri-Bold", fontSize=11.5, leading=15, textColor=AZUL,
                    spaceBefore=8, spaceAfter=3, keepWithNext=1)
cel = ParagraphStyle("cel", parent=base, fontSize=9, leading=11.5)
cab = ParagraphStyle("cab", parent=cel, fontName="Calibri-Bold", textColor=colors.white)
mono = ParagraphStyle("mono", fontName="Consolas", fontSize=7.4, leading=9.2)
LARGURA = 18 * cm


def p(texto, estilo=base):
    return Paragraph(texto, estilo)


def marca(nome):
    return f'<font color="{MARCAS[nome].hexval()}"><b>[{nome}]</b></font>'


def tabela(cabecalho, linhas, larguras):
    total = sum(larguras)
    larguras = [LARGURA * l / total for l in larguras]
    dados = [[p(c, cab) for c in cabecalho]] + [[p(str(c), cel) for c in linha] for linha in linhas]
    t = Table(dados, colWidths=larguras, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), AZUL),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.5, LINHA),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, FUNDO]),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return t


def codigo(texto, legenda=None):
    bloco = Table([[Preformatted(texto, mono)]], colWidths=[LARGURA])
    bloco.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), CODIGO),
        ("BOX", (0, 0), (-1, -1), 0.5, LINHA),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    itens = [p(f"<i>{legenda}</i>", sub)] if legenda else []
    return KeepTogether(itens + [bloco, Spacer(1, 6)])


def caixa(texto):
    t = Table([[p(texto)]], colWidths=[LARGURA])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), FUNDO),
        ("BOX", (0, 0), (-1, -1), 0.5, LINHA),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    return t


def conferir(itens):
    caixinha = '<font name="Simbolos" size="11">☐</font>'
    return tabela(["", "O que conferir"], [[caixinha, i] for i in itens], [0.7, 17.3])


def cabecalho(nome, para, resumo):
    return [
        p(nome, titulo),
        Spacer(1, 3),
        p(f"<b>Para:</b> {para}", sub),
        p(f"<b>De:</b> squad VER-3, componente S3 — Alertas e Notificações · {VERSAO}", sub),
        Spacer(1, 8),
        caixa(resumo),
        Spacer(1, 6),
        caixa(
            "<b>Como ler as marcas:</b> "
            f"{marca('Oficial')} está na Especificação de interfaces 1.0 (arquitetura 6c); "
            f"{marca('Aprovada')} decidida com as equipes, falta escrever na especificação; "
            f"{marca('Do VER-3')} regra do S3 ou do A5, que são do VER-3; "
            f"{marca('Provisória')} proposta do VER-3 que precisa da confirmação de vocês."
        ),
    ]


def gerar(arquivo, historia, rodape_nome):
    def rodape(canvas, doc):
        canvas.saveState()
        canvas.setFont("Calibri", 8.5)
        canvas.setFillColor(CINZA)
        canvas.drawString(1.5 * cm, 1.0 * cm, f"S3 (VER-3) · {rodape_nome} · {VERSAO}")
        canvas.drawRightString(A4[0] - 1.5 * cm, 1.0 * cm, f"página {doc.page}")
        canvas.restoreState()

    destino = os.path.join(SAIDA, arquivo)
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    SimpleDocTemplate(
        destino, pagesize=A4,
        leftMargin=1.5 * cm, rightMargin=1.5 * cm, topMargin=1.4 * cm, bottomMargin=1.6 * cm,
        title=rodape_nome, author="Squad VER-3",
    ).build(historia, onFirstPage=rodape, onLaterPages=rodape)
    print("gerado:", os.path.relpath(destino, RAIZ))


ENVELOPE_COMUM = [
    ["message_type", "Sim", "event, command, query ou reply"],
    ["message_id", "Sim", "UUID v4; identifica a mensagem (deduplicação)"],
    ["schema", "Sim", "nome do contrato"],
    ["schema_version", "Sim", "MAJOR.MINOR; o S3 aceita qualquer 1.x"],
    ["producer", "Sim", "id de quem publicou"],
    ["published_at", "Sim", "ISO 8601 com fuso; o S3 publica em UTC com milissegundos"],
]

# ------------------------------------------------------------------
# 1. S2

s2 = cabecalho(
    "Contrato S2 → S3: ods.s2.event",
    "VER-6 (S2 — Regras temporais e eventos)",
    "O S3 consome o <b>ods.s2.event</b> do S2 e, conforme o artefato ROUTING de cada aplicação, cria alertas. "
    "Este documento diz exatamente o que o S3 exige da mensagem e o que ele faz em cada caso, para vocês conferirem "
    "que o S2 publica do jeito que o S3 espera.",
)
s2 += [
    p("1. Envelope (perfil EV-SERVIÇO)", h2),
    tabela(["Campo", "Obrig.", "O que o S3 exige"], [l for l in ENVELOPE_COMUM if l[0] != "producer"] + [
        ["producer", "Sim", f'tem que ser <b>"S2"</b>; outro produtor é descartado {marca("Oficial")} (lista fechada, seção 9)'],
        ["captured_at", "Sim", "ISO 8601 com fuso"],
        ["session_id", "Sim", "UUID v4; é copiado para o alerta"],
        ["sequence", "Sim", "inteiro ≥ 0"],
        ["source_id", "Não", "câmera principal; é copiado para o alerta"],
        ["artifact_version", "Não", "versão das regras do S2"],
    ], [3, 1.2, 9]),
    p("2. Payload", h2),
    tabela(["Campo", "Obrig.", "Tipo e regra no S3"], [
        ["event_id", "Sim", "UUID v4, <b>o mesmo</b> em STARTED, UPDATED e ENDED; o S3 cria um alerta por event_id"],
        ["event_type", "Sim", "texto; é o que o ROUTING usa para decidir se vira alerta"],
        ["rule_id", "Sim", "texto"],
        ["status", "Sim", "CONFIRMED ou UNCERTAIN"],
        ["phase", "Sim", "STARTED, UPDATED ou ENDED"],
        ["confidence", "Sim", "número entre 0 e 1"],
        ["started_at", "Sim", "ISO 8601 com fuso"],
        ["duration_ms", "Sim", "inteiro ≥ 0"],
        ["source_ids", "Sim", "lista de textos"],
        ["entity_id, class, zone_id, equipment_item", "Não", "texto não vazio quando presentes"],
        ["details", "Não", "objeto"],
    ], [4, 1.2, 9]),
    codigo(js(ler("entrada.json")), "Exemplo aceito pelo S3"),
    p("3. O que o S3 faz em cada caso", h2),
    tabela(["Situação", "Comportamento do S3", "Origem"], [
        ["Mensagem inválida (JSON, envelope ou payload fora do contrato)", "Descartada e registrada em log. Não há reply, porque é evento", marca("Oficial")],
        ["Mesmo message_id de novo", "Descartada", f"{marca('Oficial')} (regra 7)"],
        ["event_type sem rota no ROUTING", "Ignorado; não é erro", marca("Oficial")],
        ["status UNCERTAIN", "Só vira alerta se a rota tiver min_status UNCERTAIN", marca("Oficial")],
        ["STARTED de event_id novo", "Cria o alerta", marca("Oficial")],
        ["UPDATED de event_id novo", "Cria o alerta (o STARTED se perdeu e a violação continua)", marca("Do VER-3")],
        ["UPDATED ou ENDED de event_id que já tem alerta", "O alerta não muda", marca("Do VER-3")],
        ["ENDED de event_id sem alerta", "Não cria alerta; registra em log", marca("Do VER-3")],
        ["ENDED chega antes do STARTED", "O STARTED ainda cria o alerta: a violação aconteceu e precisa de registro", marca("Do VER-3")],
        ["Fim da violação (ENDED)", "<b>Não</b> encerra o alerta: quem encerra é uma pessoa (CLOSE)", marca("Do VER-3")],
    ], [5, 8, 2.2]),
    p("4. Campos do evento que viram texto do alerta", h2),
    p("O ROUTING de cada aplicação monta title e message com <b>{campo}</b>. O S3 entende: event_id, event_type, rule_id, "
      "status, entity_id, class, zone_id, equipment_item, confidence, started_at, duration_ms, <b>duration_s</b> "
      f"(= duration_ms ÷ 1000, arredondado para baixo {marca('Do VER-3')}), source_id e qualquer chave de <b>details</b>. "
      "Campo ausente fica escrito como {campo}."),
    p("5. O que pedimos ao S2", h2),
    conferir([
        "O ods.s2.event que vocês publicam tem o envelope completo da seção 1 e producer = \"S2\".",
        "O event_id é UUID v4 e não muda entre STARTED, UPDATED e ENDED do mesmo evento.",
        f"{marca('Provisória')} Vocês publicam com <b>event_id como chave de partição</b> no Kafka, para o STARTED chegar antes do ENDED.",
        f"{marca('Do VER-3')} Vocês concordam com o comportamento da seção 3 em UPDATED e ENDED (é o que o A5 decidiu).",
        "Quando conseguirem mandar um evento real, avisem o VER-3 para o primeiro teste.",
    ]),
]

# ------------------------------------------------------------------
# 2. Aplicações

ap = cabecalho(
    "Contratos entre o S3 e as aplicações",
    "A3 (VER-6), A5 (VER-3) e A7 (VER-4). Também servem a A1, que pode criar alertas e consultar o histórico.",
    "O S3 publica o alerta (<b>ods.s3.alert</b>) e atende três pedidos das aplicações: criar alerta, agir sobre um alerta "
    "(reconhecer, silenciar, encerrar) e consultar o histórico. Cada aplicação também entrega ao S3 o seu artefato "
    "<b>ROUTING</b> e o seu <b>arquivo de permissões</b>. Os exemplos deste documento foram gerados rodando o S3.",
)
ap += [
    p("1. Resumo", h2),
    tabela(["Mensagem", "Direção", "Quem envia", "Resposta"], [
        ["ods.s3.alert", "S3 → aplicação", "S3", "—"],
        ["ods.s3.alert.command", "aplicação → S3", "A1, A3, A5, A7", "ods.s3.alert.reply"],
        ["ods.s3.alert_action.command", "aplicação → S3", "A3, A5, A7", "ods.s3.alert_action.reply"],
        ["ods.s3.alerts.query", "aplicação → S3", "A1, A3, A5, A7", "ods.s3.alerts.reply"],
        ["ROUTING (artefato)", "aplicação → S3", "A3, A5, A7", "—"],
        ["Permissões (arquivo da build)", "aplicação → S3", "A3, A5, A7", "—"],
    ], [5, 3, 4, 4.5]),
    p(f"Regras de todos os pedidos {marca('Oficial')}: target = \"S3\"; message_id UUID v4; o mesmo comando enviado de novo "
      "(mesmo message_id) recebe <b>a mesma resposta</b>, sem efeito novo; a resposta traz correlation_id = message_id do pedido. "
      f"{marca('Do VER-3')} Envelope inválido também recebe resposta (ERROR, INVALID_MESSAGE), desde que o message_id seja UUID."),

    p("2. ods.s3.alert — o alerta que vocês recebem", h2),
    tabela(["Campo do payload", "Obrig.", "Conteúdo"], [
        ["alert_id", "Sim", "UUID v4; <b>o mesmo</b> do início ao fim do alerta"],
        ["event_id", "Não", "evento do S2 que gerou o alerta; ausente em alerta criado por comando"],
        ["title, message", "Sim", "texto para exibir"],
        ["severity", "Sim", "HIGH, MEDIUM ou LOW"],
        ["recipients", "Sim", "lista de user:&lt;id&gt;, group:&lt;id&gt; ou role:&lt;id&gt;"],
        ["channels", "Sim", "lista de SOUND, SCREEN, PUSH"],
        ["clip_id", "Não", f"vídeo da ocorrência no S4; chega numa republicação, depois da criação {marca('Aprovada')}"],
        ["state", "Sim", "CREATED, ACKNOWLEDGED, MUTED ou CLOSED"],
        ["created_at", "Sim", "criação"],
        ["acknowledged_by, acknowledged_at", "Não", "quem reconheceu e quando"],
        ["muted_until", "Não", "fim do silêncio"],
        ["closed_by, closed_at", "Não", "quem encerrou e quando"],
    ], [4.5, 1.2, 9]),
    p("Quando o S3 publica", h3),
    p(f"Na criação, a cada mudança de state {marca('Oficial')} e quando o clip_id é vinculado, com o mesmo state {marca('Do VER-3')}."),
    p("Como tratar as mensagens (importante)", h3),
    tabela(["Regra", "Origem"], [
        ["Mensagem com message_id já visto: <b>descartar</b> (é repetição).", marca("Oficial")],
        ["Mensagem com alert_id já visto e message_id novo: <b>atualizar</b> o alerta (não ignorar: é ACK, MUTE, CLOSE ou clipe).", marca("Provisória")],
        ["Guardar o último sequence aplicado de cada alerta e ignorar sequence menor; o S3 usa alert_id como chave de partição.", marca("Provisória")],
        ["Alerta criado por comando chega <b>sem session_id</b>; o sequence dele conta só pelo produtor.", marca("Aprovada")],
        ["A entrega é pelo menos uma vez: a mesma mensagem (mesmo message_id) pode chegar duas vezes.", marca("Do VER-3")],
    ], [14, 2.2]),
    codigo(js(ALERTA_EVENTO), "Exemplo: alerta criado a partir de ods.s2.event (ROUTING do A5)"),
    codigo(js(ALERTA_COMANDO), "Exemplo: alerta criado por comando — sem session_id, source_id e artifact_version"),

    p("3. ods.s3.alert.command — criar alerta", h2),
    tabela(["Campo do payload", "Obrig.", "Regra"], [
        ["title, message", "Sim", "texto não vazio"],
        ["severity", "Sim", "HIGH, MEDIUM ou LOW"],
        ["recipients", "Sim", "lista não vazia de user:/group:/role:&lt;id&gt;"],
        ["channels", "Sim", "lista não vazia de SOUND, SCREEN, PUSH"],
        ["event_id, clip_id", "Não", "UUID v4"],
    ], [4, 1.2, 9]),
    p("Resposta OK: { status: OK, alert_id }. Erros: INVALID_RECIPIENT e INVALID_CHANNEL "
      f"{marca('Oficial')}; INVALID_PAYLOAD (gravidade inválida, campo ausente, UUID malformado) e INVALID_MESSAGE "
      f"(envelope) {marca('Do VER-3')}."),
    codigo(js(CRIAR), "Pedido"),
    codigo(js(REPLY_CRIAR), "Resposta"),

    p("4. ods.s3.alert_action.command — reconhecer, silenciar, encerrar", h2),
    tabela(["Campo do payload", "Obrig.", "Regra"], [
        ["alert_id", "Sim", "UUID v4"],
        ["action", "Sim", "ACKNOWLEDGE, MUTE ou CLOSE"],
        ["actor", "Sim", "user:&lt;id&gt; — quem executou; precisa ter permissão no arquivo de permissões da build"],
        ["mute_until", "Sim em MUTE", "ISO 8601 com fuso; horário no passado é aceito"],
        ["reason", "Não", "texto; fica no histórico"],
    ], [4, 2, 9]),
    p(f"Mudanças permitidas {marca('Do VER-3')}", h3),
    tabela(["Ação", "Aceita quando o alerta está em", "Novo estado"], [
        ["ACKNOWLEDGE", "CREATED", "ACKNOWLEDGED"],
        ["MUTE", "CREATED ou ACKNOWLEDGED", "MUTED"],
        ["CLOSE", "CREATED, ACKNOWLEDGED ou MUTED", "CLOSED (final)"],
    ], [4, 8, 4]),
    p("Qualquer outra mudança: ERROR INVALID_TRANSITION, sem alterar nada. Alerta inexistente: ALERT_NOT_FOUND "
      f"{marca('Oficial')}. Actor sem permissão no arquivo de permissões da build: UNAUTHORIZED_ACTOR, sem alterar nada "
      f"{marca('Do VER-3')}. Formato errado: INVALID_PAYLOAD. Quando o muted_until passa, o alerta continua MUTED: o S3 não "
      f"tem temporizador, e a aplicação usa o muted_until para voltar a avisar {marca('Do VER-3')}."),
    codigo(js(ACAO), "Pedido"),
    codigo(js(REPLY_ACAO), "Resposta OK"),
    codigo(js(REPLY_ERRO["payload"]), "Resposta de erro (o mesmo ACK enviado de novo com outro message_id) — só o payload"),

    p(f"5. Arquivo de permissões {marca('Do VER-3')}", h2),
    p("Cada build entrega ao S3 (parâmetro --permissoes) quem pode reconhecer, silenciar e encerrar. Em <b>members</b>, "
      "quem está em cada role: ou group:; em <b>actions</b>, quem pode cada ação: * (qualquer um), user:&lt;id&gt;, "
      "role:&lt;id&gt; ou group:&lt;id&gt; declarado em members. Ação fora do arquivo é negada a todos. Sem o arquivo, "
      "qualquer user:&lt;id&gt; pode agir."),
    codigo(js(ler("permissoes_a5.json")), "Modelo (contratos/Aplicações/Permissões de ação (exemplo).json)"),

    p("6. ods.s3.alerts.query — histórico", h2),
    tabela(["Filtro", "Regra"], [
        ["from_at, to_at", f"sobre created_at; from_at inclusivo, to_at exclusivo {marca('Do VER-3')}; from_at maior que to_at devolve lista vazia"],
        ["state, severity", "valores dos enums do alerta"],
        ["limit", "1 a 500; padrão 100"],
    ], [3.5, 12]),
    p("Resposta: { status, alerts: [payload de ods.s3.alert], next_from_at }. Ordem: created_at, depois alert_id. "
      "next_from_at só aparece quando há próxima página; repita a consulta com from_at = next_from_at. "
      f"A página termina antes do milissegundo do next_from_at, para não repetir alertas {marca('Do VER-3')}. "
      "Limitação: mais de limit alertas criados no mesmo milissegundo não avançam. O histórico fica no banco do S3: "
      "as aplicações consultam por esta mensagem, nunca lendo o banco direto."),
    codigo(js(REPLY_CONSULTA["payload"]), "Resposta (severity HIGH, limit 1) — só o payload"),

    p("7. Artefato ROUTING", h2),
    tabela(["Campo de cada rota", "Obrig.", "Regra"], [
        ["event_type", "Sim", "tipo do evento do S2; uma rota por tipo"],
        ["min_status", "Sim", "CONFIRMED (só confirmados) ou UNCERTAIN (também os em dúvida)"],
        ["severity, recipients, channels", "Sim", "como no alerta"],
        ["title, message", "Sim", "texto com {campo} do evento (lista no contrato do S2)"],
        ["capture", "Sim", f"true = o S3 vincula o clip_id do S4 ao alerta {marca('Aprovada')}"],
    ], [4.5, 1.2, 9]),
    p(f"Como o S3 recebe {marca('Provisória')}: um arquivo JSON por build, com a versão (vai em artifact_version de cada alerta). "
      "Versão nova = reiniciar o S3 com o arquivo novo. Rota inválida impede o S3 de subir."),
    codigo(js(ler("routing_a5.json")), "Exemplo (ROUTING do A5)"),
    p(f"<b>A7</b> {marca('Provisória')}: a build do A7 não tem S2, então o S3 dela roda <b>sem ROUTING</b> e só atende comandos."),

    p("8. O que fica com cada aplicação", h2),
    tabela(["Assunto", "Decisão", "Origem"], [
        ["Quem pode reconhecer, silenciar e encerrar", "A aplicação escreve o arquivo de permissões da build (seção 5); o S3 rejeita com UNAUTHORIZED_ACTOR quem não está nele", marca("Do VER-3")],
        ["Som e push", "O S3 só publica channels; o backend da aplicação toca o som e envia o push, para em ACK/MUTE/CLOSE e volta quando muted_until passa", f"{marca('Do VER-3')} no A5; {marca('Provisória')} em A3 e A7"],
        ["Desligamento manual (A5, RF6)", "É CLOSE (closed_by aparece no alerta); MUTE é pausa temporária", marca("Do VER-3")],
        ["Remetente não autorizado", "Bloqueado no broker pelo B3, não pelo S3", marca("Aprovada")],
    ], [4, 9.5, 3]),
    p("9. O que conferir", h2),
    conferir([
        "Vocês leem ods.s3.alert com os campos da seção 2, inclusive sem session_id e com clip_id chegando depois.",
        "Vocês atualizam o alerta pelo alert_id e descartam só message_id repetido.",
        "Os comandos e a consulta que vocês enviam batem com as seções 3, 4 e 6 (target, message_id UUID, campos).",
        "Vocês tratam as respostas ERROR com os códigos listados, inclusive UNAUTHORIZED_ACTOR.",
        "A3, A5 e A7: mandem o arquivo de permissões de vocês (seção 5).",
        "A3 e A5: o ROUTING de vocês segue a seção 7. Mandem o arquivo e a versão ao VER-3.",
        "A3: confirmem os filtros de histórico que vão usar e que leem a paginação (next_from_at).",
        "A7: confirmem que não precisam de ROUTING e que os alertas de vocês vêm só por comando.",
        "A3 e A7: confirmem que o backend de vocês cuida do som e do push.",
    ]),
]

# ------------------------------------------------------------------
# 3. Barramento

br = cabecalho(
    "S3 no barramento: tópicos, envelope e entrega",
    "TRA-1 (B1 — contratos e validador; B3 — barramento e Kafka) e TRA-2 (B2 — tempo e sessão).",
    "Como o S3 usa o barramento: que tópicos lê e escreve, com que chave, como confirma a entrega, como segue o envelope "
    "e quais exceções e códigos foram decididos e ainda precisam entrar na especificação.",
)
br += [
    p("1. Tópicos", h2),
    tabela(["Tópico", "S3", "Chave de partição", "Quem escreve (proposta de ACL)"], [
        ["ods.s2.event", "lê", "event_id (pedido ao S2)", "S2"],
        ["ods.s4.clip", f"lê {marca('Aprovada')}", "—", "S4"],
        ["ods.s3.alert.command", "lê", "—", "A1, A3, A5, A7"],
        ["ods.s3.alert_action.command", "lê", "—", "A3, A5, A7"],
        ["ods.s3.alerts.query", "lê", "—", "A1, A3, A5, A7"],
        ["ods.s3.alert", "escreve", f"alert_id {marca('Provisória')}", "só S3"],
        ["ods.s3.alert.reply, ods.s3.alert_action.reply, ods.s3.alerts.reply", "escreve", "—", "só S3"],
    ], [5.5, 2.2, 3.8, 4.5]),
    p(f"{marca('Provisória')} Nome do tópico = nome do schema (\"um tópico por schema\", frente B3). Dá para trocar por um mapa "
      "schema → tópico sem mudar o código. As respostas vão para o tópico .reply, e quem pediu acha a sua pelo correlation_id. "
      "Sem ROUTING (build do A7), o S3 lê só os três tópicos de pedido. O A2 não usa o S3."),
    p("2. Como o S3 usa o Kafka", h2),
    tabela(["Item", "Comportamento"], [
        ["Conexão", "S3_KAFKA_BOOTSTRAP (sem endereço no código); grupo S3_KAFKA_GROUP_ID, padrão S3-alertas; auto.offset.reset = earliest"],
        ["Consumo", "Commit manual do offset <b>depois</b> de processar; se o processamento falhar, a mensagem volta"],
        ["Publicação", "Cada mudança de alerta e a mensagem a publicar são gravadas juntas no SQLite do S3 (transactional outbox) e publicadas depois, na ordem; para na primeira falha e tenta de novo na partida e a cada volta do consumidor"],
        ["Confirmação", "Só conta como entregue com o relatório on_delivery sem erro; fila vazia no flush não basta"],
        ["Garantia", "Pelo menos uma vez: no reenvio, a mesma mensagem com o mesmo message_id"],
        ["Rastreabilidade", "Uma linha de log por mensagem que entra, sai ou é descartada, com message_id, correlation_id, alert_id e resultado"],
        ["Uso do broker de testes", "python -m s3 exige --tempo-limite ou --max-mensagens (Guia do Kafka, 4.5)"],
    ], [3.5, 12]),
    p("3. Envelope", h2),
    tabela(["Mensagem do S3", "Perfil", "Observação"], [
        ["ods.s3.alert (de evento)", "EV-SERVIÇO", "session_id, captured_at e source_id do evento; correlation_id = event_id; artifact_version = versão do ROUTING"],
        ["ods.s3.alert (de comando)", "EV-SERVIÇO com exceção", f"<b>sem session_id</b>; sequence conta só pelo produtor; correlation_id = message_id do comando; captured_at = criação {marca('Aprovada')}"],
        ["respostas .reply", "RPL", "correlation_id = message_id do pedido; sem campos de sinal"],
    ], [4, 3, 9]),
    p("Validação no S3 da entrada: identificadores UUID v4; schema_version 1.x; horários ISO 8601 com fuso. "
      f"sequence por producer + session_id, guardado no SQLite: não volta a 0 no reinício {marca('Oficial')}."),
    p("4. Códigos de erro do S3", h2),
    tabela(["Código", "Quando", "Origem"], [
        ["ALERT_NOT_FOUND", "alert_action para alerta inexistente", marca("Oficial")],
        ["INVALID_TRANSITION", "ação não permitida no estado atual", marca("Oficial")],
        ["INVALID_RECIPIENT", "destinatário fora de user:/group:/role:", marca("Oficial")],
        ["INVALID_CHANNEL", "canal fora de SOUND/SCREEN/PUSH", marca("Oficial")],
        ["INVALID_PAYLOAD", "payload fora do contrato (gravidade, campo ausente, UUID, action, actor, limit…)", f"{marca('Do VER-3')} pedir inclusão na seção 7"],
        ["INVALID_MESSAGE", "envelope fora do contrato", f"{marca('Do VER-3')} pedir inclusão na seção 7"],
        ["UNAUTHORIZED_ACTOR", "actor sem permissão para a ação, pelo arquivo de permissões da build", f"{marca('Do VER-3')} pedir inclusão na seção 7"],
    ], [4, 9, 3.5]),
    p("5. Remetentes", h2),
    p(f"{marca('Aprovada')} Quem bloqueia remetente fora da lista é o <b>B3</b>, no broker. O S3 não confere o producer dos pedidos "
      "(só exige producer = S2 em ods.s2.event e S4 em ods.s4.clip). Pedido: ACL por tópico e credencial por aplicação, "
      "conforme a última coluna da tabela 1. A pessoa que age (actor) é conferida pelo S3, pelo arquivo de permissões da build."),
    p("6. Para a TRA-2 (B2 — sessão)", h2),
    tabela(["Assunto", "Decisão", "Origem"], [
        ["ods.session.started", "O S3 não consome (lista fechada da seção 9); o histórico de alertas não zera em sessão nova", marca("Provisória")],
        ["Alerta sem sessão", "Alerta criado por comando sai sem session_id", marca("Aprovada")],
    ], [3.5, 10, 3]),
    p("7. O que pedimos", h2),
    conferir([
        "B3: confirmar o nome dos tópicos e o tópico das respostas.",
        "B3: confirmar alert_id como chave de partição de ods.s3.alert.",
        "B3: configurar as ACLs da tabela 1.",
        "B3: passar o endereço do broker e uma janela para um teste curto.",
        "B1: escrever na especificação a exceção de session_id em alerta criado por comando (perfil EV-SERVIÇO e validador).",
        "B1: incluir o S3 entre os consumidores de ods.s4.clip (seção 9).",
        "B1: aceitar a republicação do alerta quando o clip_id é vinculado (a seção 7 só fala em mudança de state).",
        "B1: incluir INVALID_PAYLOAD, INVALID_MESSAGE e UNAUTHORIZED_ACTOR na tabela de erros do S3.",
        "B1: confirmar a leitura da regra 7: descartar pelo message_id e atualizar pelo alert_id/event_id.",
        "B2: confirmar que o S3 não precisa consumir ods.session.started.",
    ]),
]

if __name__ == "__main__":
    gerar(os.path.join("S2", "CONTRATO_S3_PARA_S2.pdf"), s2, "Contrato S2 → S3")
    gerar(os.path.join("Aplicações", "CONTRATOS_S3_PARA_APLICACOES.pdf"), ap, "Contratos S3 ↔ aplicações")
    gerar(os.path.join("Barramentos", "CONTRATOS_S3_PARA_BARRAMENTO.pdf"), br, "S3 no barramento")
