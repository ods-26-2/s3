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
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

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


historia = [
    p("S3 — Situação da Sprint 3", titulo),
    Spacer(1, 3),
    p("Squad VER-3 · Componente S3 — Alertas e Notificações · Arquitetura 6c (Especificação de interfaces 1.0)", sub),
    p(f"{DATA} · Código: {VERSAO}", sub),
    Spacer(1, 8),
    numeros([
        (TESTES, "testes passando<br/>(suíte sem broker)"),
        ("7", "entregas da sprint<br/>(S3.1 a S3.7)"),
        ("6", "com o código do S3<br/>pronto"),
        ("3", "cenários passando<br/>num Kafka local"),
        ("0", "testadas no<br/>broker oficial"),
    ]),
    Spacer(1, 8),
    caixa(
        "<b>Resumo:</b> o S3 está implementado e já rodou contra um <b>Kafka de verdade</b> (local, no Docker): "
        "fluxo S2 → S3 → A5 completo, queda do broker e queda do próprio S3, sem perder nem duplicar mensagens. "
        "O S2, o S4 e o A5 foram simulados. "
        "<b>O que falta é a integração real:</b> o endereço do broker da TRA-1, o S2 e as aplicações publicando de "
        "verdade, o validador de B1 e o backend do A5 demonstrando som, tela e push (S3.5). "
        "Para destravar o A5 sem esperar ninguém, já existe um kit para o backend testar contra o S3 no Kafka local."
    ),

    p("1. O que a Sprint 3 pede e a situação de cada entrega", h2),
    tabela(["Entrega", "Implementado", "Testado localmente", "Testado com integração real", "Situação"], [
        ["<b>S3.1</b> Contratos e ROUTING", SIM, SIM, NAO, QUASE],
        ["<b>S3.2</b> Comunicação pelo barramento B3", SIM, SIM + " (Kafka local)", NAO, QUASE],
        ["<b>S3.3</b> Banco de alertas e histórico", SIM, SIM, NAO, FEITO],
        ["<b>S3.4</b> Comandos e ciclo de vida", SIM, SIM, NAO, FEITO],
        ["<b>S3.5</b> Canais SOUND, SCREEN e PUSH", PARTE + " (o S3 publica os canais)", PARTE, NAO, PARCIAL],
        ["<b>S3.6</b> Confiabilidade da publicação", SIM, SIM + " (Kafka local)", NAO, FEITO],
        ["<b>S3.7</b> Evidência e associação ao evento", SIM, SIM + " (com o exemplo real do S4)", NAO, FEITO],
        ["<b>Testes de integração</b> S2 → S3 → A5, A3, A7", "—", SIM + " (S2 e A5 simulados)", NAO, PARCIAL],
    ], [5.2, 2.6, 3.0, 2.8, 2.2]),
    Spacer(1, 4),
    p("<b>O que falta para o aceite de cada entrega</b>", h3),
    tabela(["Entrega", "Aceite oficial", "O que falta"], [
        ["S3.1", "Exemplos e cenários de B1 passam; PPE_NON_COMPLIANT/CONFIRMED gera o alerta; inválidas rejeitadas",
         "Os cenários de B1: o validador oficial ainda não foi publicado. Hoje usamos um verificador próprio, escrito da especificação"],
        ["S3.2", "ods.s2.event no broker chega ao S3; ods.s3.alert chega ao backend do A5",
         "Já provado num Kafka local, com S2 e A5 simulados. Falta o endereço do broker oficial (TRA-1) e o S2 e o A5 reais"],
        ["S3.3", "Dados sobrevivem ao reinício; consulta com filtros e paginação; reentrega não duplica",
         "Nada no código. Combinar com o P3 (SIS-1) onde fica o arquivo do banco na build"],
        ["S3.4", "ACK, MUTE e CLOSE pela interface oficial; não autorizados rejeitados; persistidos e republicados",
         "Os arquivos de permissões de A3, A5 e A7 (quem pode reconhecer, silenciar e encerrar)"],
        ["S3.5", "Cada canal alcança o destinatário; MUTE e CLOSE refletem na notificação",
         "O backend do A5 (VER-3) demonstrar a entrega com o kit; confirmar que A3 e A7 cuidam do som e do push"],
        ["S3.6", "Após queda e recuperação do publicador, entrega sem duplicação",
         "Provado num Kafka local (queda do broker e do S3). Repetir na janela do broker oficial"],
        ["S3.7", "A5 associa alerta, evento e evidência pelos campos oficiais",
         "O S4 aplicar as 4 correções no ods.s4.clip (testadas com o exemplo real deles); o A5 demonstrar a associação"],
    ], [1.8, 7, 8.7]),

    p("2. O que já foi feito", h2),
    tabela(["Tema", "O que existe", "Onde"], [
        ["Contratos 6c", "Os 9 contratos exigidos do S3 mais o ods.s4.clip; envelope de 13 campos; perfis EV-SERVIÇO, CMD e RPL; UUID v4 e horários ISO validados", "src/s3/envelope.py, contracts.py, commands.py, output.py"],
        ["ROUTING", "Carregado de arquivo, com versão; min_status, severidade, destinatários, canais, textos com {campo} e capture", "src/s3/routing.py"],
        ["Banco e histórico", "SQLite com alertas, histórico de ações, deduplicação, sequence e fila de saída; sobrevive ao reinício; consulta com filtros e paginação", "src/s3/repository_sqlite.py"],
        ["Confiabilidade", "Transactional outbox: mudança e mensagem gravadas juntas; reenvio na ordem com o mesmo message_id; confirmação pelo relatório de entrega do Kafka", "src/s3/service.py, kafka_adapter.py"],
        ["Ciclo de vida", "ACK, MUTE e CLOSE com transições, auditoria e republicação; regras de UPDATED/ENDED aprovadas pelo A5", "src/s3/service.py"],
        ["Autorização", "Arquivo de permissões por build (--permissoes); negado = UNAUTHORIZED_ACTOR", "src/s3/permissoes.py"],
        ["Evidência", "Consome ods.s4.clip e vincula o clip_id; clipe antes ou depois do alerta; vale o primeiro. Exemplo real do S4 testado: rejeitado por 4 motivos, aceito com as correções", "src/s3/ingestion.py, tests/s3/test_exemplo_s4.py"],
        ["Rastreabilidade", "Uma linha de log por mensagem que entra, sai ou é descartada, com message_id, correlation_id e alert_id", "src/s3/rastreio.py"],
        ["Kafka", "Adaptador confluent-kafka, um tópico por schema, chave alert_id, commit depois de processar", "src/s3/kafka_adapter.py, __main__.py"],
        ["Kafka local", "Kafka 3.8.0 no Docker; roteiro que sobe o S3 e simula S2, S4 e A5: fluxo completo, queda do broker e queda do S3, tudo passando", "infra/kafka-local/, tools/teste_kafka_local.py, docs/TESTE_KAFKA_LOCAL.md"],
        ["Kit para as aplicações", "O backend do A5 (ou A3, A7) testa contra o S3 no Kafka local: sobe o S3, simula S2 e S4 e publica os cenários D1–D5 de consumo", "tools/kit_aplicacoes.py,<br/>docs/KIT_APLICACOES_KAFKA_LOCAL.md"],
        ["Testes", f"{TESTES} testes, sem broker; persistência em memória e com SQLite; teste integrado S2 → S3 → A5 com broker em memória; verificador independente do contrato", "tests/s3/"],
        ["Comunicação", "Contratos enviados às equipes; contratos/Barramentos sem o campo comment; mensagens para TRA-1, S4 e A5 prontas", "contratos/"],
        ["Decisões", "Cada pendência com decisão registrada: aprovada, do VER-3 ou provisória", "docs/DECISOES_S3.md"],
    ], [2.6, 8.8, 6.1]),
    Spacer(1, 4),
    p("<b>Respostas recebidas</b>", h3),
    tabela(["Equipe", "Resposta", "Efeito"], [
        ["A5 (VER-3)", "Contratos ok", "Regras do ciclo de vida confirmadas"],
        ["A2 (VER-2)", "Não vai usar o S3", "Sai das verificações"],
        ["A3 (VER-6)", "Vai precisar do histórico", "Usa ods.s3.alerts.query; falta saber se quer a lista de alertas ou a trilha de ações"],
        ["S4 (MOD-2)", "Campos alinhados; clipe chega depois do alerta; media_uri resolvido pelo S4", "Confirma o desenho; o exemplo real ainda é rejeitado (4 correções, já testadas)"],
        ["B1/B2 (aprovações)", "session_id opcional em alerta de comando; S3 consome ods.s4.clip; B3 bloqueia remetente", "Falta escrever na especificação"],
    ], [3, 7.5, 7]),

    p("3. O que depende de resposta de outras equipes", h2),
    tabela(["Equipe", "O que precisamos", "Bloqueia"], [
        ["<b>TRA-1 (B3)</b>", "Endereço do broker e janela de teste; autenticação e grupo de consumidores; nome dos tópicos e das respostas; chave alert_id; ACL por tópico", "S3.2 e S3.6 no broker oficial e todos os testes de integração"],
        ["<b>TRA-1 (B1)</b>", "Escrever na especificação: session_id opcional, S3 consumidor de ods.s4.clip, republicação ao vincular o clipe, códigos INVALID_PAYLOAD, INVALID_MESSAGE e UNAUTHORIZED_ACTOR; texto da regra 7 (deduplicação); validador oficial", "S3.1 (cenários de B1)"],
        ["<b>TRA-2 (B2)</b>", "Confirmar que o S3 não consome ods.session.started", "Nada (provisória)"],
        ["<b>S2 (VER-6)</b>", "Publicar ods.s2.event de verdade; usar event_id como chave de partição; avisar quando estiver pronto", "S3.2 e o fluxo S2 → S3 → A5 real"],
        ["<b>A3 (VER-6)</b>", "ROUTING, arquivo de permissões, filtros do histórico e se quer a trilha de ações; confirmar som e push", "S3.4, S3.5 e o teste com A3"],
        ["<b>A7 (VER-4)</b>", "Confirmar que não usa ROUTING; arquivo de permissões; confirmar som e push", "S3.4, S3.5 e o teste com A7"],
        ["<b>S4 (MOD-2)</b>", "Aplicar as 4 correções: captured_at, sequence, session_id UUID e clip_id sem prefixo", "S3.7 com dados reais"],
        ["<b>P3 (SIS-1)</b>", "Onde fica o arquivo SQLite do S3 na build", "Implantação"],
    ], [2.8, 10, 4.7]),

    p("4. O que você pode fazer sozinho", h2),
    p("<b>Feito desde o relatório anterior</b>", h3),
    lista([
        ("Commitar a correção do clipe pendente", "Commit 17750d4, junto com o teste integrado local S2 → S3 → A5"),
        ("Instalar o Docker Desktop", "Docker Desktop 4.97.0 funcionando"),
        ("Teste num Kafka local", "Fluxo completo, queda do broker e queda do S3 passando (tools/teste_kafka_local.py --queda)"),
        ("Testes com o exemplo real do S4", "Exemplo guardado sem alteração; cada um dos 4 problemas rejeita sozinho; a versão corrigida é aceita"),
        ("Kit para as aplicações", "tools/kit_aplicacoes.py e o guia; cenários D1–D5 conferidos no Kafka local"),
        ("README atualizado", "Contagem de testes, Kafka local e kit"),
    ], feito=True),
    p("<b>Próximos</b>", h3),
    lista([
        ("Mandar as três mensagens", "TRA-1: acesso ao broker e o que escrever na especificação, com contratos/Barramentos sem comment. S4: as 4 correções. Backend do A5: como usar o kit"),
        ("S3.5 no A5 (squad VER-3)", "O backend do A5 usar o kit: consumir ods.s3.alert, tocar o som, mostrar na tela, mandar o push e parar em ACK, MUTE e CLOSE"),
        ("Cobrar as respostas", "A3 (ROUTING, permissões, histórico), A7 (sem ROUTING, permissões), S2 (publicar, chave event_id), TRA-2 (session.started), P3 (local do banco)"),
        ("Teste na janela do broker oficial", "Quando a TRA-1 passar o endereço: o mesmo fluxo do teste local, com --tempo-limite, e uma queda controlada"),
        ("Limpar documentos desatualizados", "docs/PERGUNTAS_EQUIPES_S3.pdf ficou para trás"),
    ]),

    p("5. Escolhas em aberto", h2),
    tabela(["Escolha", "Recomendação", "Por quê"], [
        ["Banco: SQLite ou banco de servidor", "SQLite, um arquivo por instância", "Uma instância do S3 por build e um consumidor por processo"],
        ["Republicar o alerta ao vincular o clipe", "Manter e pedir confirmação ao B1", "Sem isso, o clipe só aparece na consulta de histórico"],
        ["Paginação com mais de limit alertas no mesmo milissegundo", "Aceitar como limitação documentada", "Caso raro; a solução seria um cursor novo no contrato"],
        ["Nova versão do ROUTING", "Reiniciar o S3 por enquanto", "ods.artifact.published não tem contrato de acesso ao registro"],
        ["S3 sem arquivo de permissões", "Hoje só avisa; tornar obrigatório depois que as três aplicações entregarem", "Para nenhuma build parar de funcionar de repente"],
        ["Trilha de ações para o A3", "Perguntar antes de criar qualquer coisa", "Não sai por nenhum contrato hoje; seria um contrato novo"],
    ], [5, 5.5, 7]),

    p("6. Riscos", h2),
    tabela(["Risco", "Efeito", "Como reduzir"], [
        ["O broker oficial não chegar a tempo", "S3.2 e S3.6 sem prova no broker oficial", "Já há prova num Kafka local (Docker); pedir o endereço já"],
        ["Nomes de tópicos diferentes no broker oficial", "O S3 assina e publica nos nomes errados", "Hoje os nomes são os dos schemas e ficam no código; trocar é uma mudança pequena, mas precisa da resposta da TRA-1 antes da janela"],
        ["S2 e aplicações não ficarem prontos", "Sem fluxo S2 → S3 → A5 real", "Kit para o backend do A5 testar sem o S2; roteiro do Kafka local como evidência"],
        ["Especificação não atualizada pela TRA-1", "Validador de B1 rejeitar alerta sem session_id e os códigos extras", "Mandar a lista exata do que escrever (está na mensagem e no PDF do barramento)"],
        ["Aplicações descartarem pelo alert_id", "Perdem ACK, MUTE e CLOSE", "Cenários D1–D4 do kit mostram o comportamento certo"],
    ], [4.5, 5.5, 7.5]),
]


def rodape(canvas, doc):
    canvas.saveState()
    canvas.setFont("Calibri", 8.5)
    canvas.setFillColor(CINZA)
    canvas.drawString(1.5 * cm, 1.0 * cm, f"S3 (VER-3) · Situação da Sprint 3 · {DATA}")
    canvas.drawRightString(A4[0] - 1.5 * cm, 1.0 * cm, f"página {doc.page}")
    canvas.restoreState()


SimpleDocTemplate(
    SAIDA, pagesize=A4, leftMargin=1.5 * cm, rightMargin=1.5 * cm, topMargin=1.4 * cm, bottomMargin=1.6 * cm,
    title="S3 — Situação da Sprint 3", author="Squad VER-3",
).build(historia, onFirstPage=rodape, onLaterPages=rodape)
print("gerado:", SAIDA)
