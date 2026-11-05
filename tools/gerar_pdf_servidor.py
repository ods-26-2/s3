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
BROKER = "172.18.4.56:9092"  # Kafka da Jetson, só na rede do DComp (e-mail do professor)
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
# Imagem ARM: atualize depois de testar (docker buildx, emulação arm64).
ARM_SITUACAO = FEITO
ARM_DETALHE = "05/11: imagem arm64 construída e rodada por emulação; passou na verificação inteira. Arquivo para levar: ods-s3-arm64.tar (47 MB)"
ARM_TABELA = tabela(["No teste em arm64 (05/11, emulação no Docker Desktop)", "Resultado"], [
    ["Construir a imagem para linux/arm64", "ok, em 96 s"],
    ["Dentro do contêiner", "aarch64, Python 3.12, librdkafka 2.16"],
    ["Rodar o S3 da imagem arm64 contra o Kafka local e passar na verificação", "tudo ok (4 comandos, 1 consulta)"],
    ["Arquivo para a Jetson sem internet", "ods-s3-arm64.tar, 47 MB, nome ods-s3:latest, linux/arm64"],
], [9, 8.5])

ETAPAS = [
    ("Código pronto e testado", FEITO, "Imagem Docker testada contra o Kafka local; 394 testes passando"),
    ("Script de instalação testado", FEITO, "Num Ubuntu 24.04 limpo: instalou o Docker, criou os 9 tópicos num Kafka vazio, ligou o S3 e passou na verificação"),
    ("Verificação mínima no broker", FEITO, "tools/verificar_broker.py, dentro da imagem; passou contra Kafka de teste"),
    ("Integração contínua (GitHub Actions)", FEITO, "Testes, cobertura e imagem a cada push; passando no GitHub"),
    ("Broker da Jetson", FEITO, f"E-mail do professor: {BROKER}, só na rede do DComp, a partir de 06/11. Nenhuma mudança no código"),
    ("Imagem para ARM (Jetson)", ARM_SITUACAO, ARM_DETALHE),
    ("Juntar na main", FALTA, "Ajustes para a Jetson (compose, --sem-construir, documentos)"),
    ("Acesso à Jetson", FALTA, "Usuário e SSH, se tem Docker e internet (professor / TRA-1)"),
    ("Entrar na Jetson e rodar o script", FALTA, "De dentro da rede do DComp: 3 comandos, uns 3 minutos"),
    ("Conferir e rodar a verificação no broker", FALTA, "Na Jetson, logo depois do script; tirar a captura"),
]

historia = [
    p("S3 — Passo a passo para rodar no servidor (Jetson)", titulo),
    Spacer(1, 3),
    p(f"Squad VER-3 · Componente S3 — Alertas e Notificações · Broker do projeto: Kafka da Jetson ({BROKER}, rede do DComp)", sub),
    p(f"{DATA} · Código: {VERSAO}", sub),
    Spacer(1, 8),
    caixa(
        f"<b>Resumo:</b> o professor configurou o Kafka na Jetson (<b>{BROKER}</b>, acessível só dentro da rede do "
        "DComp) e pediu os componentes prontos para testar e usar com o restante da aplicação a partir de 06/11. "
        "O S3 não precisa de mudança no código: o endereço é configuração. Foi preparado para a Jetson: imagem em "
        "ARM, instalação do <font face='Consolas'>docker compose</font> se faltar e um caminho sem internet "
        "(imagem construída fora e carregada). <b>O que falta é o acesso à Jetson</b> (usuário e SSH); com ele, "
        "a instalação é um comando, de dentro da rede do DComp."
    ),

    p("Situação", h2),
    tabela(["#", "Etapa", "Situação", "Detalhe"],
           [[str(i), nome, sit, det] for i, (nome, sit, det) in enumerate(ETAPAS, start=1)],
           [0.8, 5.1, 2.2, 9.4]),

    *etapa(1, "Código pronto e testado", FEITO, "Na máquina de desenvolvimento",
           p("A imagem foi construída e rodada contra o Kafka local, sem tocar no broker do projeto. Resultado:"),
           tabela(["Verificação", "Resultado"], [
               ["Evento do S2 vira alerta, gravado e publicado", "ok"],
               ["Banco no volume sobrevive ao reinício do contêiner", "ok, sem reprocessar nada"],
               ["<font face='Consolas'>docker stop</font> (SIGTERM)", "sai em ~1 s, fechando o consumidor e o banco"],
               ["Ao religar, processa um evento novo", "em ~5 s"],
               ["Recursos parado", "18 MB de memória, 0,04% de CPU; imagem de 200 MB"],
               ["Usuário dentro do contêiner", "s3 (uid 1000), não root"],
               ["Suíte de testes", "394 testes passando, 98% de cobertura, nenhum usa o broker"],
           ], [9, 8.5])),

    *etapa(2, "Script de instalação testado", FEITO, "infra/servidor/instalar.sh",
           p("Teste: um contêiner Ubuntu 24.04 limpo, sem Docker, fez o papel do servidor, e um Kafka separado, "
             "sem nenhum tópico, fez o papel do broker do projeto."),
           tabela(["O script", "No teste"], [
               ["Instala o Docker, se faltar", "instalou (Docker 29.9 e Compose 5.7)"],
               ["Instala o plugin <font face='Consolas'>docker compose</font>, se faltar (comum na Jetson)", "acrescentado depois do teste, para a Jetson"],
               ["Guarda o broker em <font face='Consolas'>infra/servidor/.env</font> (fora do git)", "ok; depois de down e up, religou sem export"],
               ["Cria no broker os tópicos do S3 que faltam, antes de ligar o S3", "criou os 9; na segunda execução, \"já existe\" nos 9"],
               ["Constrói a imagem e liga o <font face='Consolas'>ods-s3</font>", "ok, contêiner Up"],
               ["Com <font face='Consolas'>--sem-construir</font>, usa a imagem já carregada", "para a Jetson sem internet (etapa 6)"],
           ], [9, 8.5])),

    *etapa(3, "Verificação mínima no broker", FEITO, "tools/verificar_broker.py (vai dentro da imagem)",
           p("Prova o S3 no broker real com 4 comandos e 1 consulta. Faz o papel do A5 (aplicação do VER-3), sem se "
             "passar pelo S2: confere os 9 tópicos, cria um alerta de teste por comando (\"[TESTE S3]\"), espera o "
             "ods.s3.alert, dá ACKNOWLEDGE e CLOSE (o alerta de teste não fica aberto nas aplicações) e consulta o "
             "histórico. Fora de localhost, só roda com <font face='Consolas'>--broker-oficial</font>."),
           tabela(["No teste", "Resultado"], [
               ["Com o S3 ligado (Kafka de teste, fora de localhost, com --broker-oficial)", "tudo ok em ~4 s"],
               ["Endereço fora de localhost sem --broker-oficial", "recusa antes de conectar"],
               ["Com o S3 desligado", "falha em 15 s: \"não chegou a resposta... O S3 está ligado?\""],
           ], [9, 8.5])),

    *etapa(4, "Integração contínua (GitHub Actions)", FEITO, ".github/workflows/ci.yml",
           p("A cada push e pull request, o GitHub roda a suíte com cobertura e constrói a imagem. Roda nos servidores "
             "do GitHub, sem usar o broker. O resultado de cada push aparece na aba <b>Actions</b> do repositório.")),

    *etapa(5, "Broker da Jetson", FEITO, "E-mail do professor",
           tabela(["Ponto", "Situação"], [
               ["Endereço", f"<b>{BROKER}</b>, acessível só dentro da rede do DComp"],
               ["Desde quando", "testes e uso com o restante da aplicação a partir de 06/11"],
               ["Mudança no S3", "nenhuma no código: o endereço vem do argumento do script (ou de S3_KAFKA_BOOTSTRAP)"],
               ["Broker anterior", "nuvem de testes do Guia do Kafka (34.66.235.91:9093)"],
           ], [5, 12.5])),

    *etapa(6, "Imagem para ARM (Jetson)", ARM_SITUACAO, "A Jetson é ARM (aarch64), não um PC comum",
           p("A imagem parte do <font face='Consolas'>python:3.12-slim</font>, que existe para ARM, e a "
             "<font face='Consolas'>confluent-kafka</font> tem versão para ARM; o script constrói a imagem na própria "
             "Jetson. Use o Docker, não o Python da Jetson: o JetPack 5 vem com Python 3.8, e o S3 precisa de 3.10 ou mais."),
           ARM_TABELA,
           Spacer(1, 4),
           p("<b>Sem internet na Jetson</b>, construa a imagem ARM num PC com Docker e leve o arquivo:"),
           codigo(f"""
# no PC (Docker Desktop), na pasta do s3:
docker buildx build --platform linux/arm64 --output "type=docker,name=ods-s3:latest,dest=ods-s3-arm64.tar" .
# copie ods-s3-arm64.tar e a pasta do s3 para a Jetson; lá:
docker load -i ods-s3-arm64.tar
bash infra/servidor/instalar.sh {BROKER} --sem-construir
""")),

    p("<b>Plano B, no notebook com Windows</b> (na rede do DComp, com o Docker Desktop aberto). O S3 fica no ar "
      "só enquanto o notebook estiver ligado e lá. Testado em 05/11 contra um Kafka sem tópicos:"),
    codigo(rf"""
powershell -ExecutionPolicy Bypass -File infra\servidor\ligar_windows.ps1 {BROKER}
docker exec ods-s3 python tools/verificar_broker.py --broker-oficial
"""),

    *etapa(7, "Juntar na main", FALTA, "Na sua máquina",
           codigo("""
git add -A
git commit -m "S3 pronto para a Jetson: imagem ARM, compose e instalação sem internet"
git push
"""),
           Spacer(1, 3),
           p("Depois, juntar na main (pull request). A Jetson baixa o código da main.")),

    *etapa(8, "Acesso à Jetson", FALTA, "Com o professor ou a TRA-1",
           tabela(["Pergunta", "Por quê"], [
               ["Qual usuário e como entrar (SSH, senha ou chave)?", "Sem acesso não há como instalar"],
               ["Cada squad instala o seu componente na Jetson, ou alguém centraliza?", "Para não haver dois S3 ligados ao mesmo broker"],
               ["A Jetson tem Docker? E o docker compose?", "O script instala o compose se faltar, mas precisa de sudo"],
               ["A Jetson alcança a internet (GitHub, Docker Hub, PyPI)?", "Se não, usar o caminho sem internet (etapa 6)"],
           ], [8.5, 9])),

    *etapa(9, "Entrar na Jetson e rodar o script", FALTA, "De dentro da rede do DComp",
           codigo(f"""
ssh USUARIO@IP_DA_JETSON                       # usuário e IP: etapa 8
git clone https://github.com/ods-26-2/s3.git && cd s3
bash infra/servidor/instalar.sh {BROKER}
"""),
           Spacer(1, 3),
           p("O script mostra a arquitetura (<font face='Consolas'>aarch64</font> na Jetson), usa sudo para instalar "
             "o que faltar, cria os tópicos e termina com <b>ok: 9 tópicos do S3 conferidos</b> e as últimas linhas do "
             "log. Para fazer tudo à mão: <font face='Consolas'>docs/IMPLANTACAO_SERVIDOR.md</font>.")),

    *etapa(10, "Conferir e rodar a verificação no broker", FALTA, "Na Jetson, na pasta s3",
           codigo(f"""
docker compose -f {COMPOSE} ps          # ods-s3 deve estar "Up"
docker exec ods-s3 python tools/verificar_broker.py --broker-oficial
"""),
           Spacer(1, 3),
           p("O esperado é terminar com <b>Tudo certo</b>; tire a captura: é a evidência do S3.2 no broker real. "
             "Depois, quando o S2 ou o A5 mandarem algo, o log mostra <font face='Consolas'>entrada ... "
             "resultado=alerta_criado</font> e <font face='Consolas'>saida ... state=CREATED</font>.")),

    p("No dia a dia", h2),
    tabela(["Para", "Comando, na Jetson (na pasta s3)"], [
        ["Ver o log", f"<font face='Consolas'>docker compose -f {COMPOSE} logs -f s3</font>"],
        ["Parar (o banco fica guardado)", f"<font face='Consolas'>docker compose -f {COMPOSE} down</font>"],
        ["Ligar de novo (lê o broker do .env)", f"<font face='Consolas'>docker compose -f {COMPOSE} up -d</font>"],
        ["Atualizar o código", f"<font face='Consolas'>bash infra/servidor/instalar.sh {BROKER} --atualizar</font>"],
        ["Contar os alertas no banco", "<font face='Consolas'>docker exec ods-s3 python -c \"import sqlite3; "
                                       "print(sqlite3.connect('/dados/s3.sqlite3').execute('select count(*) from alertas').fetchone())\"</font>"],
    ], [4.6, 12.9]),

    p("Regras importantes", h2),
    caixa(
        "<b>Um S3 só.</b> Com o S3 no servidor, não ligue outro S3 contra o mesmo broker (nem na sua máquina): dois S3 "
        "respondem aos mesmos comandos, cada um com o seu banco. O <font face='Consolas'>kit_aplicacoes.py</font> e o "
        "<font face='Consolas'>teste_kafka_local.py</font> recusam qualquer broker que não seja localhost, de propósito.<br/><br/>"
        "<b>O broker é compartilhado por todos os squads.</b> O S3 espera 1 s entre leituras, sem sobrecarregar a Jetson. "
        "A TRA-1 liberou deixar o S3 ligado (05/11).",
        fundo=ALERTA,
    ),

    p("Problemas comuns", h2),
    tabela(["O que aparece", "Causa", "O que fazer"], [
        ["<i>uso: bash infra/servidor/instalar.sh &lt;broker host:porta&gt;</i>", "Faltou o endereço do broker", f"<font face='Consolas'>bash infra/servidor/instalar.sh {BROKER}</font>"],
        ["<i>defina S3_KAFKA_BOOTSTRAP</i>", "Compose rodado sem o .env (antes do script)", "Rodar o script uma vez; ele cria o .env"],
        ["<i>permission denied ... docker.sock</i>", "O usuário ainda não está no grupo docker", "Saia e entre de novo pelo ssh, ou use sudo na frente do comando"],
        ["<i>AVISO: o broker não respondeu</i>", "Fora da rede do DComp, ou Kafka da Jetson parado", "Rodar de dentro da rede do DComp; testar com <font face='Consolas'>nc -vz 172.18.4.56 9092</font>; falar com o professor ou a TRA-1"],
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
