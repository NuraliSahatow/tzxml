
FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml pdm.lock ./
COPY elektronika_products_20240421_155417.xml ./
RUN pip install pdm \
    && pdm install --prod

COPY src/ .

CMD ["pdm", "run", "python", "main.py"]