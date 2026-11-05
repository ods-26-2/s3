"""
Gera o passo a passo para rodar o S3 num servidor:

    docs/PASSO_A_PASSO_SERVIDOR_S3.pdf

A situação de cada etapa (FEITO / FALTA) é escrita à mão em ETAPAS:
atualize quando avançar. A versão (branch e commit) sai do git.

Uso, na raiz do repositório (Windows: usa as fontes Calibri, Consolas e
Segoe UI Symbol do sistema):

    pip install reportlab          # só para este script; não é dependência do S3
    python tools/gerar_pdf_servidor.py
"""

import datetime
import os
import subprocess

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    KeepTogether, Paragraph, Preformatted, SimpleDocTemplate, Spacer, Table, TableStyle,
)

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAIDA = os.path.join(RAIZ, "docs", "PASSO_A_PASSO_SERVIDOR_S3.pdf")
DATA = f"{datetime.date.today():%d/%m/%Y}"
BROKER = "34.66.235.91:9093"
COMPOSE = "infra/servidor/docker-compose.yml"


def _git(*argumentos):
    try:
        return subprocess.run(
            ["git", *argumentos], cwd=RAIZ, capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "?"


COMMIT = _git("rev-parse", "--short", "origin/main")
VERSAO = f"main no GitHub, commit {COMMIT}"

pdfmetrics.registerFont(TTFont("Calibri", "C:/Windows/Fonts/calibri.ttf"))
pdfmetrics.registerFont(TTFont("Calibri-Bold", "C:/Windows/Fonts/calibrib.ttf"))
pdfmetrics.registerFont(TTFont("Consolas", "C:/Windows/Fonts/consola.ttf"))
pdfmetrics.registerFont(TTFont("Simbolos", "C:/Windows/Fonts/seguisym.ttf"))
pdfmetrics.registerFontFamily("Calibri", normal="Calibri", bold="Calibri-Bold")

AZUL = colors.HexColor("#15324e")
CINZA = colors.HexColor("#4a5666")
LINHA = colors.HexColor("#d9dee5")
FUNDO = colors.HexColor("#eef3f9")
CODIGO = colors.HexColor("#f4f6f8")
VERDE = colors.HexColor("#2e7d4f")
LARANJA = colors.HexColor("#b26b00")
ALERTA = colors.HexColor("#fff4e5")

base = ParagraphStyle("base", fontName="Calibri", fontSize=10, leading=13.5)
titulo = ParagraphStyle("t", parent=base, fontName="Calibri-Bold", fontSize=21, leading=25, textColor=AZUL)
sub = ParagraphStyle("s", parent=base, fontSize=10.5, textColor=CINZA)
h2 = ParagraphStyle("h2", parent=base, fontName="Calibri-Bold", fontSize=14, leading=18, textColor=AZUL,
                    spaceBefore=12, spaceAfter=4, keepWithNext=1)
cel = ParagraphStyle("cel", parent=base, fontSize=9, leading=11.5)
cab = ParagraphStyle("cab", parent=cel, fontName="Calibri-Bold", textColor=colors.white)
mono = ParagraphStyle("mono", fontName="Consolas", fontSize=8.3, leading=10.6)
LARGURA = 18 * cm


def p(texto, estilo=base):
    return Paragraph(texto, estilo)


def cor(texto, c):
    return f'<font color="{c.hexval()}"><b>{texto}</b></font>'


FEITO = '<font name="Simbolos" color="#2e7d4f">☑</font> ' + cor("FEITO", VERDE)
FALTA = '<font name="Simbolos">☐</font> ' + cor("FALTA", LARANJA)


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


def codigo(texto):
    t = Table([[Preformatted(texto.strip("\n"), mono)]], colWidths=[LARGURA])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), CODIGO),
        ("BOX", (0, 0), (-1, -1), 0.5, LINHA),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    return t


def etapa(numero, nome, situacao, onde, *conteudo):
    cabecalho = p(f"{numero}. {nome} &nbsp;&nbsp;<font size='10'>{situacao}</font>", h2)
    onde = p(f"<font color='{CINZA.hexval()}'>{onde}</font>")
    return [KeepTogether([cabecalho, onde, Spacer(1, 3), *conteudo[:2]]), *conteudo[2:]]


# Situação de cada etapa: atualize aqui quando avançar.
ETAPAS = [
    ("Código pronto e testado", FEITO, "Imagem Docker testada contra o Kafka local em 04/11; 385 testes passando"),
    ("Script de instalação testado", FEITO, "infra/servidor/instalar.sh rodado num Ubuntu 24.04 limpo em 04/11: instalou o Docker, ligou o S3 e processou um evento"),
    ("Verificação mínima no broker", FEITO, "tools/verificar_broker.py, dentro da imagem; passou contra o Kafka local em 4 s"),
    ("Integração contínua (GitHub Actions)", FEITO, "Testes, cobertura e imagem a cada push; a primeira execução no GitHub passou (04/11, commit ea1ae2d)"),
    ("Juntar na main", FALTA, f"Script e CI já no GitHub (feature/docker); falta a verificação e o merge. A main está em {COMMIT}"),
    ("Descobrir o servidor e como entrar nele", FALTA, "Com quem pediu o S3 no servidor"),
    ("Confirmar dois pontos com a TRA-1", FALTA, "Na aula de 05/11"),
    ("Entrar no servidor e rodar o script", FALTA, "No servidor: 3 comandos, uns 3 minutos"),
    ("Conferir e rodar a verificação no broker", FALTA, "No servidor, na janela combinada com a TRA-1"),
]

historia = [
    p("S3 — Passo a passo para rodar no servidor", titulo),
    Spacer(1, 3),
    p("Squad VER-3 · Componente S3 — Alertas e Notificações · Broker do projeto (Guia do Kafka da TRA-1)", sub),
    p(f"{DATA} · Código: {VERSAO}", sub),
    Spacer(1, 8),
    caixa(
        "<b>Resumo:</b> o S3 está pronto para o servidor. A imagem Docker e o script de instalação foram testados "
        "(o script, num Ubuntu limpo, sem Docker), e o GitHub passa a rodar os testes sozinho a cada push. Endereço do "
        f"broker: <b>{BROKER}</b>, sem autenticação; nomes dos tópicos aprovados pela TRA-1 (os da especificação). "
        "<b>O que falta é ter o servidor:</b> qual máquina e como entrar nela. Com isso, a instalação é um comando e "
        "leva uns 3 minutos de máquina ligada."
    ),

    p("Situação", h2),
    tabela(["#", "Etapa", "Situação", "Detalhe"],
           [[str(i), nome, sit, det] for i, (nome, sit, det) in enumerate(ETAPAS, start=1)],
           [0.6, 5.2, 2.2, 9.5]),

    *etapa(1, "Código pronto e testado", FEITO, "Na máquina de desenvolvimento",
           p("A imagem foi construída e rodada contra o Kafka local, sem tocar no broker oficial. Resultado:"),
           tabela(["Verificação", "Resultado"], [
               ["Evento do S2 vira alerta, gravado e publicado", "ok"],
               ["Banco no volume sobrevive ao reinício do contêiner", "ok, sem reprocessar nada"],
               ["<font face='Consolas'>docker stop</font> (SIGTERM)", "sai em ~1 s, fechando o consumidor e o banco"],
               ["Ao religar, processa um evento novo", "em ~5 s"],
               ["Recursos parado", "18 MB de memória, 0,04% de CPU; imagem de 200 MB"],
               ["Usuário dentro do contêiner", "s3 (uid 1000), não root"],
               ["Suíte de testes", "385 testes passando, nenhum usa o broker"],
           ], [9, 8.5])),

    *etapa(2, "Script de instalação testado", FEITO, "infra/servidor/instalar.sh",
           p("O script faz sozinho o que antes eram três etapas à mão. Teste: um contêiner Ubuntu 24.04 limpo fez o "
             "papel do servidor, e um Kafka separado fez o papel do broker oficial."),
           tabela(["O script", "No teste"], [
               ["Instala o Docker, se faltar", "instalou (Docker 29.9 e Compose 5.7)"],
               ["Guarda o broker em <font face='Consolas'>infra/servidor/.env</font> (fora do git)", "ok; depois de down e up, religou sem export"],
               ["Constrói a imagem e liga o <font face='Consolas'>ods-s3</font>", "ok, contêiner Up"],
               ["Confere os tópicos do S3 no broker (só lê)", "ok: 9 tópicos"],
               ["Mostra o log e os comandos do dia a dia", "ok"],
               ["S3 instalado processa um evento", "evento publicado virou alerta (alerta_criado, CREATED)"],
           ], [9, 8.5])),

    *etapa(3, "Verificação mínima no broker", FEITO, "tools/verificar_broker.py (vai dentro da imagem)",
           p("Um comando, para rodar uma vez no servidor, que prova o S3 no broker real gastando o mínimo. Faz o papel do "
             "A5 (aplicação do VER-3), sem se passar pelo S2: confere os 9 tópicos, cria um alerta de teste por comando "
             "(\"[TESTE S3]\"), espera o ods.s3.alert, dá ACKNOWLEDGE e CLOSE (o alerta de teste não fica aberto nas "
             "aplicações) e consulta o histórico. São 4 comandos e 1 consulta; os consumidores começam do fim dos tópicos."),
           tabela(["No teste contra o Kafka local (04/11)", "Resultado"], [
               ["Com o S3 ligado", "tudo ok em 4 s; 4 mensagens enviadas, 7 recebidas"],
               ["Endereço fora de localhost sem --broker-oficial", "recusa antes de conectar"],
               ["Com o S3 desligado", "falha em 15 s: \"não chegou a resposta... O S3 está ligado?\""],
           ], [9, 8.5])),

    *etapa(4, "Integração contínua (GitHub Actions)", FEITO, ".github/workflows/ci.yml",
           p("A cada push e pull request, o GitHub roda a suíte com cobertura e constrói a imagem do servidor. "
             "Roda nos servidores do GitHub: <b>não gasta os créditos do professor</b> nem usa o broker. "
             "Os mesmos passos foram conferidos num contêiner Linux com Python 3.12: 385 testes passando, 98% de "
             "cobertura, imagem construída e S3 abrindo dentro dela. <b>A primeira execução no GitHub passou</b> "
             "(04/11, commit ea1ae2d); o resultado de cada push aparece na aba <b>Actions</b> do repositório.")),

    *etapa(5, "Juntar na main", FALTA, "Na sua máquina (o script e o CI já foram, no commit ea1ae2d)",
           codigo("""
git add -A
git commit -m "Verificação mínima no broker e relatórios atualizados"
git push
"""),
           Spacer(1, 3),
           p("Depois, juntar na main (pull request), como da outra vez. O servidor baixa o código da main.")),

    *etapa(6, "Descobrir o servidor e como entrar nele", FALTA, "Com quem pediu o S3 no servidor (professor, TRA-1 ou P3/SIS-1)",
           tabela(["Pergunta", "Por quê"], [
               ["Qual máquina? (endereço IP ou nome; Google Cloud ou outra)", "É para onde você vai entrar"],
               ["Como entrar? (usuário e senha, ou chave SSH)", "Sem acesso não há como instalar nada"],
               ["Qual sistema operacional?", "O script é para Ubuntu ou Debian"],
               ["A máquina tem saída para a internet e para o broker?", f"Para baixar o código e alcançar {BROKER}; não precisa abrir porta de entrada"],
           ], [8.5, 9])),

    *etapa(7, "Confirmar dois pontos com a TRA-1", FALTA, "Na aula de 05/11",
           tabela(["Pergunta", "Se a resposta for não"], [
               ["Os tópicos do S3 já existem no broker, ou o broker cria na primeira mensagem?",
                "O S3 liga, mas não recebe nada, e guarda o que tentar publicar na fila de saída. Pedir à TRA-1 para criar os tópicos. O script avisa quando faltam"],
               ["Pode deixar o S3 ligado o tempo todo no broker pago?",
                "Ligar só na janela de teste e desligar depois (comando de parar, abaixo)"],
           ], [8, 9.5])),

    *etapa(8, "Entrar no servidor e rodar o script", FALTA, "No seu terminal, depois dentro do servidor",
           codigo(f"""
ssh usuario@ENDERECO_DO_SERVIDOR
sudo apt-get install -y git                    # só se o git faltar
git clone https://github.com/ods-26-2/s3.git && cd s3
bash infra/servidor/instalar.sh {BROKER}
"""),
           Spacer(1, 3),
           p("O script usa sudo para instalar o Docker, se precisar. No fim, ele mostra os tópicos "
             "encontrados (<b>ok: 9 tópicos</b> é o esperado) e as últimas linhas do log. "
             "Para fazer tudo à mão, os comandos estão em <font face='Consolas'>docs/IMPLANTACAO_SERVIDOR.md</font>.")),

    *etapa(9, "Conferir e rodar a verificação no broker", FALTA, "Dentro do servidor, na pasta s3",
           codigo(f"""
docker compose -f {COMPOSE} ps          # ods-s3 deve estar "Up"
docker compose -f {COMPOSE} logs -f s3  # Ctrl+C sai do log; o S3 continua ligado
"""),
           Spacer(1, 3),
           Spacer(1, 3),
           p("Na janela combinada com a TRA-1, a verificação (etapa 3). O esperado é terminar com <b>Tudo certo</b>; "
             "tire a captura: é a evidência do S3.2 no broker real."),
           codigo("docker exec ods-s3 python tools/verificar_broker.py --broker-oficial"),
           Spacer(1, 3),
           p("Depois, quando o S2 ou o A5 mandarem algo, o log mostra <font face='Consolas'>entrada ... resultado=alerta_criado</font> "
             "e <font face='Consolas'>saida ... state=CREATED</font>.")),

    p("No dia a dia", h2),
    tabela(["Para", "Comando, dentro do servidor (na pasta s3)"], [
        ["Ver o log", f"<font face='Consolas'>docker compose -f {COMPOSE} logs -f s3</font>"],
        ["Parar (o banco fica guardado)", f"<font face='Consolas'>docker compose -f {COMPOSE} down</font>"],
        ["Ligar de novo (lê o broker do .env)", f"<font face='Consolas'>docker compose -f {COMPOSE} up -d</font>"],
        ["Atualizar o código", f"<font face='Consolas'>bash infra/servidor/instalar.sh {BROKER} --atualizar</font>"],
        ["Contar os alertas no banco", "<font face='Consolas'>docker exec ods-s3 python -c \"import sqlite3; "
                                       "print(sqlite3.connect('/dados/s3.sqlite3').execute('select count(*) from alertas').fetchone())\"</font>"],
    ], [4.6, 12.9]),

    p("Para economizar os créditos", h2),
    tabela(["O quê", "Por quê"], [
        ["Usar a menor máquina possível (1 a 2 GB de RAM, como uma <font face='Consolas'>e2-small</font> no Google Cloud)",
         "O S3 usa 18 MB de memória e quase nada de CPU parado"],
        ["Desligar a máquina (não apagar) quando ninguém estiver testando",
         "O banco fica no disco e o S3 volta sozinho quando a máquina liga (restart: unless-stopped)"],
        ["Rodar o script de uma vez, sem testar comando por comando no servidor",
         "A instalação foi ensaiada num Ubuntu limpo: é a máquina ligada pelo menor tempo"],
        ["Testes e CI no GitHub, não no servidor", "O CI roda na cota gratuita do GitHub"],
    ], [9, 8.5]),

    p("Regras importantes", h2),
    caixa(
        "<b>Um S3 só.</b> Com o S3 no servidor, não ligue outro S3 contra o mesmo broker (nem na sua máquina): dois S3 "
        "respondem aos mesmos comandos, cada um com o seu banco. O <font face='Consolas'>kit_aplicacoes.py</font> e o "
        "<font face='Consolas'>teste_kafka_local.py</font> recusam qualquer broker que não seja localhost, de propósito.<br/><br/>"
        "<b>O broker é pago e compartilhado pelos 15 squads.</b> O S3 espera 1 s entre leituras, sem martelar o broker. "
        "Se a TRA-1 pedir para desligar fora do horário de teste, use o comando de parar.",
        fundo=ALERTA,
    ),

    p("Problemas comuns", h2),
    tabela(["O que aparece", "Causa", "O que fazer"], [
        ["<i>uso: bash infra/servidor/instalar.sh &lt;broker host:porta&gt;</i>", "Faltou o endereço do broker", f"<font face='Consolas'>bash infra/servidor/instalar.sh {BROKER}</font>"],
        ["<i>defina S3_KAFKA_BOOTSTRAP</i>", "Compose rodado sem o .env (antes do script)", "Rodar o script uma vez; ele cria o .env"],
        ["<i>permission denied ... docker.sock</i>", "O usuário ainda não está no grupo docker", "Saia e entre de novo pelo ssh, ou use sudo na frente do comando"],
        ["<i>AVISO: nenhum tópico do S3 no broker</i>", "Tópicos inexistentes, ou broker inalcançável", "Falar com a TRA-1; testar a rede com <font face='Consolas'>nc -vz 34.66.235.91 9093</font>"],
        ["S3 ligado, mas nada chega no log", "Ninguém publicando ainda", "Normal até o S2 e o A5 publicarem"],
        ["Depois de um reinício, demora até ~45 s para processar", "O S3 anterior foi morto sem fechar (queda de energia, kill)", "Esperar: o Kafka libera o lugar sozinho. Com docker stop ou down isso não acontece"],
    ], [5, 5.2, 7.3]),
]

def rodape(canvas, doc):
    canvas.saveState()
    canvas.setFont("Calibri", 8.5)
    canvas.setFillColor(CINZA)
    canvas.drawString(1.5 * cm, 1.0 * cm, f"S3 (VER-3) · Passo a passo para o servidor · {DATA}")
    canvas.drawRightString(A4[0] - 1.5 * cm, 1.0 * cm, f"página {doc.page}")
    canvas.restoreState()


SimpleDocTemplate(
    SAIDA, pagesize=A4, leftMargin=1.5 * cm, rightMargin=1.5 * cm, topMargin=1.4 * cm, bottomMargin=1.6 * cm,
    title="S3 — Passo a passo para o servidor", author="Squad VER-3",
).build(historia, onFirstPage=rodape, onLaterPages=rodape)
print("gerado:", SAIDA)
