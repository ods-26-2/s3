# Imagem do S3 — Alertas e Notificações (VER-3).
#
#   docker build -t ods-s3 .
#
# Roda o `python -m s3` ligado ao broker de S3_KAFKA_BOOTSTRAP. O padrão é o
# Kafka da Jetson (172.18.4.56:9092, rede do DComp), como pediu o professor;
# outro broker: -e S3_KAFKA_BOOTSTRAP=host:porta. Alertas, histórico e fila de
# saída ficam no SQLite em /dados: monte um volume ali para não perder nada ao
# recriar o contêiner. Para a equipe de implantação: docs/IMPLANTACAO_EQUIPE.md.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    S3_KAFKA_BOOTSTRAP=172.18.4.56:9092

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
# Verificação mínima no broker real, depois da instalação:
#   docker exec ods-s3 python tools/verificar_broker.py --broker-oficial
# Criação dos tópicos do S3 antes de ligar (instalar.sh, ligar_windows.ps1):
#   docker run --rm --entrypoint python ods-s3 tools/criar_topicos.py <broker>
COPY tools/verificar_broker.py tools/criar_topicos.py tools/

# Configuração de exemplo do A5 (a mesma de contratos/). Para usar outra,
# monte arquivos por cima de /config/routing.json e /config/permissoes.json.
COPY ["contratos/Barramentos/ROUTING.json", "/config/routing.json"]
COPY ["contratos/Aplicações/Permissões de ação (exemplo).json", "/config/permissoes.json"]

RUN useradd --create-home --uid 1000 s3 && mkdir /dados && chown s3 /dados
USER s3
VOLUME ["/dados"]

# Roda continuamente (--tempo-limite de 10 anos: o S3 exige um limite, pensado
# para o broker pago da nuvem). --criar-topicos: cria os tópicos do S3 que
# faltarem antes de assinar. Reinício (always/unless-stopped) só para quedas.
ENTRYPOINT ["python", "-m", "s3"]
CMD ["--routing", "/config/routing.json", "--versao-routing", "routing-a5-1", \
     "--permissoes", "/config/permissoes.json", \
     "--banco", "/dados/s3.sqlite3", "--criar-topicos", "--tempo-limite", "315360000"]
