# Imagem do S3 — Alertas e Notificações (VER-3).
#
#   docker build -t ods-s3 .
#
# Roda o `python -m s3` ligado ao broker de S3_KAFKA_BOOTSTRAP (obrigatório,
# nunca fixo na imagem). Alertas, histórico e fila de saída ficam no SQLite
# em /dados: monte um volume ali para não perder nada ao recriar o contêiner.
# Passo a passo: docs/IMPLANTACAO_SERVIDOR.md.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/

# Configuração de exemplo do A5 (a mesma de contratos/). Para usar outra,
# monte arquivos por cima de /config/routing.json e /config/permissoes.json.
COPY ["contratos/Barramentos/ROUTING.json", "/config/routing.json"]
COPY ["contratos/Aplicações/Permissões de ação (exemplo).json", "/config/permissoes.json"]

RUN useradd --create-home --uid 1000 s3 && mkdir /dados && chown s3 /dados
USER s3
VOLUME ["/dados"]

# --tempo-limite: o S3 sai sozinho a cada 24 h (com o consumidor fechado e o
# banco salvo) e a política de reinício do contêiner o liga de novo.
ENTRYPOINT ["python", "-m", "s3"]
CMD ["--routing", "/config/routing.json", "--versao-routing", "routing-a5-1", \
     "--permissoes", "/config/permissoes.json", \
     "--banco", "/dados/s3.sqlite3", "--tempo-limite", "86400"]
