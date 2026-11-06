"""
Gera o relatório de situação da Sprint 3 do S3:

    docs/SITUACAO_SPRINT3_S3.pdf

O número de testes sai de rodar a suíte (tests/s3, sem broker) e a
versão (branch e commit), do git. O resto do texto é escrito à mão
aqui: atualize-o quando a situação mudar.

Uso, na raiz do repositório (Windows: usa as fontes Calibri e
Segoe UI Symbol do sistema):

    pip install reportlab          # só para este script; não é dependência do S3
    python tools/gerar_status_sprint3.py
"""

import datetime
import os
import re
import subprocess
import sys

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Paragraph, Preformatted, SimpleDocTemplate, Spacer, Table, TableStyle

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAIDA = os.path.join(RAIZ, "docs", "SITUACAO_SPRINT3_S3.pdf")
DATA = f"{datetime.date.today():%d/%m/%Y}"


def _git(*argumentos):
    try:
        return subprocess.run(
            ["git", *argumentos], cwd=RAIZ, capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "?"


def _testes():
    """Roda a suíte e devolve o número de testes que passaram."""

    ambiente = dict(os.environ, PYTHONPATH=os.path.join(RAIZ, "src"))
    ambiente.pop("S3_KAFKA_BOOTSTRAP", None)
    resultado = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/s3", "-q"],
        cwd=RAIZ, env=ambiente, capture_output=True, text=True,
    )
    final = resultado.stdout.strip().splitlines()[-1]
    if resultado.returncode != 0 or "failed" in final or "error" in final:
        sys.exit(f"A suíte não passou, relatório não gerado: {final}")
    return re.search(r"(\d+) passed", final).group(1)


TESTES = _testes()
VERSAO = f"branch {_git('branch', '--show-current')}, commit {_git('rev-parse', '--short', 'HEAD')}"

pdfmetrics.registerFont(TTFont("Calibri", "C:/Windows/Fonts/calibri.ttf"))
pdfmetrics.registerFont(TTFont("Calibri-Bold", "C:/Windows/Fonts/calibrib.ttf"))
pdfmetrics.registerFont(TTFont("Simbolos", "C:/Windows/Fonts/seguisym.ttf"))
pdfmetrics.registerFont(TTFont("Consolas", "C:/Windows/Fonts/consola.ttf"))
pdfmetrics.registerFontFamily("Calibri", normal="Calibri", bold="Calibri-Bold")

AZUL = colors.HexColor("#15324e")
CINZA = colors.HexColor("#4a5666")
LINHA = colors.HexColor("#d9dee5")
FUNDO = colors.HexColor("#eef3f9")
VERDE = colors.HexColor("#2e7d4f")
LARANJA = colors.HexColor("#b26b00")
VERMELHO = colors.HexColor("#b42318")

base = ParagraphStyle("base", fontName="Calibri", fontSize=10, leading=13.5)
titulo = ParagraphStyle("t", parent=base, fontName="Calibri-Bold", fontSize=21, leading=25, textColor=AZUL)
sub = ParagraphStyle("s", parent=base, fontSize=10.5, textColor=CINZA)
h2 = ParagraphStyle("h2", parent=base, fontName="Calibri-Bold", fontSize=14.5, leading=18, textColor=AZUL,
                    spaceBefore=12, spaceAfter=4, keepWithNext=1)
h3 = ParagraphStyle("h3", parent=base, fontName="Calibri-Bold", fontSize=11.5, leading=15, textColor=AZUL,
                    spaceBefore=7, spaceAfter=3, keepWithNext=1)
cel = ParagraphStyle("cel", parent=base, fontSize=9, leading=11.5)
cab = ParagraphStyle("cab", parent=cel, fontName="Calibri-Bold", textColor=colors.white)
num = ParagraphStyle("num", parent=base, fontName="Calibri-Bold", fontSize=20, leading=23, textColor=AZUL, alignment=1)
numl = ParagraphStyle("numl", parent=base, fontSize=8.5, leading=10.5, textColor=CINZA, alignment=1)
LARGURA = 18 * cm


def p(texto, estilo=base):
    return Paragraph(texto, estilo)


def cor(texto, c):
    return f'<font color="{c.hexval()}"><b>{texto}</b></font>'


FEITO = cor("Feito", VERDE)
QUASE = cor("Quase", VERDE)
PARCIAL = cor("Parcial", LARANJA)
SIM = cor("sim", VERDE)
NAO = cor("não", VERMELHO)
PARTE = cor("em parte", LARANJA)


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


mono = ParagraphStyle("mono", fontName="Consolas", fontSize=8.3, leading=10.6)


def codigo(texto):
    t = Table([[Preformatted(texto.strip("\n"), mono)]], colWidths=[LARGURA])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f4f6f8")),
        ("BOX", (0, 0), (-1, -1), 0.5, LINHA),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    return t


def caixa(texto, fundo=FUNDO):
    t = Table([[p(texto)]], colWidths=[LARGURA])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), fundo),
        ("BOX", (0, 0), (-1, -1), 0.5, LINHA),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    return t


def numeros(itens):
    linha_num = [p(n, num) for n, _ in itens]
    linha_txt = [p(t, numl) for _, t in itens]
    t = Table([linha_num, linha_txt], colWidths=[LARGURA / len(itens)] * len(itens))
    t.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.5, LINHA),
        ("LINEBEFORE", (1, 0), (-1, -1), 0.5, LINHA),
        ("BACKGROUND", (0, 0), (-1, -1), FUNDO),
        ("TOPPADDING", (0, 0), (-1, 0), 6),
        ("BOTTOMPADDING", (0, 1), (-1, 1), 6),
    ]))
    return t


def lista(itens, feito=False):
    marca = '<font name="Simbolos" size="11" color="#2e7d4f">☑</font>' if feito \
        else '<font name="Simbolos" size="11">☐</font>'
    return tabela(["", "Tarefa", "Detalhe"], [[marca, a, b] for a, b in itens], [0.7, 5.3, 12])


FEITO_S3 = cor("Feito (parte do S3)", VERDE)

historia = [
    p("S3 — Encerramento da Sprint 3", titulo),
    Spacer(1, 3),
    p("Squad VER-3 · Componente S3 — Alertas e Notificações · Arquitetura 6c (Especificação de interfaces 1.0)", sub),
    p(f"{DATA} · Código: {VERSAO}", sub),
    Spacer(1, 8),
    numeros([
        (TESTES, "testes automatizados<br/>(nenhum usa o broker)"),
        ("98%", "do código coberto<br/>pelos testes"),
        ("7 de 7", "entregas com a parte<br/>do S3 pronta"),
        ("ARM", "imagem pronta<br/>para a Jetson"),
        ("CI", "testes e imagem a cada<br/>push no GitHub"),
    ]),
    Spacer(1, 8),
    caixa(
        "<b>Resumo:</b> o S3 recebe os eventos do S2, cria os alertas pelas regras de cada aplicação (ROUTING), "
        "publica para as aplicações com os canais de cada alerta (SOUND, SCREEN, PUSH) e atende os comandos de "
        "reconhecer, silenciar e encerrar, com autorização por pessoa, histórico em banco e entrega confiável mesmo "
        "com quedas. Tudo passa pelo Kafka. Na Sprint 3, o S3 foi testado contra um Kafka real (local), "
        "demonstrado de ponta a ponta e <b>preparado para a implantação automática na Jetson</b>: imagem em ARM, já "
        "configurada com o broker da Jetson (172.18.4.56:9092), que cria os tópicos sozinha e roda continuamente."
    ),

    p("1. Entregas da Sprint 3", h2),
    tabela(["Entrega", "O que o S3 faz", "Evidência", "Situação"], [
        ["<b>S3.1</b> Contratos e ROUTING",
         "Os 9 contratos do S3 e o ods.s4.clip; envelope de 13 campos; ROUTING versionado por aplicação",
         "contratos/; verificador de contrato em cada teste; TRA-1: contratos compatíveis", QUASE],
        ["<b>S3.2</b> Comunicação pelo barramento",
         "Lê e publica pelo Kafka, um tópico por schema (aprovado pela TRA-1), com a ordem de cada alerta garantida",
         "Teste no Kafka real local, com queda do broker; imagem pronta para a Jetson", QUASE],
        ["<b>S3.3</b> Banco e histórico",
         "SQLite com alertas, histórico de ações e fila de saída; consulta com filtros e paginação",
         "Testes de persistência e reinício; demonstração do banco", FEITO],
        ["<b>S3.4</b> Comandos e ciclo de vida",
         "ACK, MUTE e CLOSE com transições válidas e autorização por pessoa; recusa o que não pode",
         "Demonstração: MUTE negado (UNAUTHORIZED_ACTOR), ACK depois do CLOSE recusado", FEITO],
        ["<b>S3.5</b> Canais SOUND, SCREEN e PUSH",
         "Cada alerta sai com os canais e os destinatários da regra; as aplicações executam cada canal",
         "Alertas publicados com channels e recipients; contratos com as aplicações", FEITO_S3],
        ["<b>S3.6</b> Confiabilidade da publicação",
         "Fila de saída gravada junto com a mudança; reenvio na ordem depois de queda; nada duplica",
         "Teste com queda do broker e do S3, sem perder nem duplicar", FEITO],
        ["<b>S3.7</b> Evidência e evento",
         "Vincula o clipe do S4 ao alerta do mesmo evento, mesmo se chegar antes",
         "Testes com o exemplo real do S4; demonstração do clipe vinculado", FEITO],
    ], [3.6, 6.2, 5.6, 2.1]),
    Spacer(1, 4),
    p("<b>Quase:</b> o código está pronto; falta só a etapa de outra equipe: o validador oficial de contratos (S3.1) "
      "e a implantação na Jetson (S3.2), que é automática, feita pela equipe especializada.", sub),

    p("2. Roteiro da apresentação (10 min)", h2),
    tabela(["", "O que mostrar", "Como"], [
        ["1", "<b>O que é o S3</b> (30 s)", "O resumo acima: evento do S2 → alerta → aplicações; comandos; histórico"],
        ["2", "<b>As entregas</b> (2 min)", "A tabela da seção 1"],
        ["3", "<b>Demonstração</b> (4 min)", "Seção 3: com o Kafka da Jetson, ou o roteiro do Kafka local"],
        ["4", "<b>Pronto para a Jetson</b> (1 min)", "Seção 4: a imagem já vem configurada; a equipe de implantação só roda"],
        ["5", "<b>Qualidade</b> (1 min)", f"Seção 5: {TESTES} testes, 98% de cobertura, CI verde no GitHub"],
        ["6", "<b>Próximos passos</b> (1 min)", "Seção 6"],
    ], [0.7, 5, 11.8]),

    p("3. Demonstração", h2),
    p("<b>A. Com o Kafka da Jetson</b> (notebook na rede do DComp, Docker Desktop aberto). Antes, "
      "<font face='Consolas'>Test-NetConnection 172.18.4.56 -Port 9092</font> deve dar <b>True</b>."),
    codigo(r"""
powershell -ExecutionPolicy Bypass -File infra\servidor\ligar_windows.ps1 172.18.4.56:9092
docker exec ods-s3 python tools/verificar_broker.py --broker-oficial
docker compose -f infra/servidor/docker-compose.yml logs -f s3
"""),
    Spacer(1, 3),
    p("Mostrar o <b>\"Tudo certo\"</b> da verificação (o S3 criou um alerta, aceitou ACK e CLOSE e respondeu a "
      "consulta pelo Kafka da Jetson) e o log ao vivo, uma linha por mensagem."),
    Spacer(1, 4),
    p("<b>B. Sem a rede da Jetson</b>: o roteiro do Kafka local (docs/ROTEIRO_DEMONSTRACAO_S3.md), com as capturas "
      "da demonstração de 31/10:"),
    tabela(["Passo", "O que aparece"], [
        ["Evento do S2 (capacete, doca-3)", "alerta criado, gravado no banco e entregue à aplicação"],
        ["Evento UNCERTAIN; UPDATED e ENDED do mesmo evento", "sem alerta; não duplica; não encerra sozinho"],
        ["Clipe do S4", "alerta republicado com o clip_id"],
        ["ACK da maria; MUTE da ana; MUTE e CLOSE da maria; ACK depois do CLOSE",
         "OK; UNAUTHORIZED_ACTOR; OK; OK; INVALID_TRANSITION"],
        ["Banco e consulta ao histórico", "CREATED → ACKNOWLEDGED → MUTED → CLOSED, com autor e motivo"],
        ["S3 desligado recebe um evento e volta", "o alerta é criado na volta: nada se perde"],
    ], [8, 9.5]),

    p("4. Pronto para a Jetson", h2),
    tabela(["Ponto", "Como está"], [
        ["Implantação", "Automática, pela equipe especializada, a partir do GitHub. Instruções: docs/IMPLANTACAO_EQUIPE.md"],
        ["Imagem", "Dockerfile no repositório; o CI publica a imagem pronta em ghcr.io/ods-26-2/s3 (ARM e PC) a cada push na main"],
        ["Processador ARM da Jetson", "Imagem arm64 construída e testada por emulação: passou na verificação inteira"],
        ["Endereço do broker", "Já vem configurado: Kafka da Jetson, 172.18.4.56:9092 (pode ser trocado por variável)"],
        ["Tópicos", "O S3 cria os que faltarem ao ligar (TRA-1 liberou)"],
        ["Funcionamento contínuo", "Roda sem parar; volta sozinho depois de uma queda; parada limpa em ~1 s"],
        ["Recursos", "~18 MB de memória e CPU quase nula parado"],
        ["Plano B", "No notebook, na rede do DComp: infra/servidor/ligar_windows.ps1 (testado)"],
    ], [4.5, 13]),

    p("5. Qualidade", h2),
    tabela(["Item", "Resultado"], [
        ["Testes automatizados", f"{TESTES}: 265 unitários, 128 de persistência (SQLite real, reinício e falhas) e 8 de integração (o S3 inteiro com Kafka simulado)"],
        ["Cobertura", "98% das 1.302 linhas do S3"],
        ["Broker nos testes", "nenhum teste acessa broker: o Kafka da nuvem era pago e compartilhado pelos 15 squads"],
        ["Integração contínua", "a cada push e pull request, o GitHub roda os testes com cobertura e constrói a imagem"],
        ["Prova de que os testes pegam erro", "desligar de propósito o reenvio da fila de saída ou a chave de partição faz os testes falharem"],
        ["Rastreabilidade", "uma linha de log por mensagem que entra, sai ou é descartada"],
    ], [4.5, 13]),
    Spacer(1, 4),
    tabela(["Também entregue", "Onde"], [
        ["Contratos enviados às aplicações, ao barramento e ao S2", "contratos/"],
        ["Decisões registradas, com origem (aprovada, do VER-3 ou provisória)", "docs/DECISOES_S3.md"],
        ["Kafka local, teste com quedas e kit para as aplicações testarem contra o S3", "infra/kafka-local/, tools/"],
        ["Roteiro de demonstração e guias de implantação", "docs/"],
    ], [11, 6.5]),

    p("6. Próximos passos", h2),
    tabela(["O quê", "Com quem"], [
        ["S3 rodando na Jetson", "Equipe de implantação (a imagem está pronta no GitHub)"],
        ["Fluxo real S2 → S3 → aplicações, com eventos de verdade", "S2 publicar no Kafka da Jetson"],
        ["Validar as mensagens do S3 no validador oficial; escrever na especificação o combinado em 21/10", "TRA-1 (B1)"],
        ["Aceitar comandos só de A3, A5 e A7 (a opção já existe, desligada)", "TRA-1: concordar"],
        ["Clipe real vinculado ao alerta", "S4: ajustar 4 campos do ods.s4.clip"],
    ], [11, 6.5]),
]


def rodape(canvas, doc):
    canvas.saveState()
    canvas.setFont("Calibri", 8.5)
    canvas.setFillColor(CINZA)
    canvas.drawString(1.5 * cm, 1.0 * cm, f"S3 (VER-3) · Encerramento da Sprint 3 · {DATA}")
    canvas.drawRightString(A4[0] - 1.5 * cm, 1.0 * cm, f"página {doc.page}")
    canvas.restoreState()


SimpleDocTemplate(
    SAIDA, pagesize=A4, leftMargin=1.5 * cm, rightMargin=1.5 * cm, topMargin=1.4 * cm, bottomMargin=1.6 * cm,
    title="S3 — Encerramento da Sprint 3", author="Squad VER-3",
).build(historia, onFirstPage=rodape, onLaterPages=rodape)
print("gerado:", SAIDA)
