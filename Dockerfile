FROM python:3.13-slim

WORKDIR /app

RUN pip install uv
RUN apt-get update && apt-get install -y wkhtmltopdf && rm -rf /var/lib/apt/lists/*

COPY ./.python-version ./pyproject.toml README.md .
RUN uv sync

# Копирование исходного кода
COPY . .

EXPOSE 8501 4200 8080