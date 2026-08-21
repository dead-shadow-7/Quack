# Quack crawler — a batch job, not a service. Runs to completion and exits.
#   docker build -t quack-crawler ./crawler
#   docker run --rm -e MONGO_URI=mongodb://mongo:27017/ quack-crawler
FROM python:3.12-slim

# Unbuffered so the progress bars stream to `docker logs` instead of
# appearing all at once at exit; UTF-8 so the emoji/box-drawing output works.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONIOENCODING=utf-8

WORKDIR /app

# Dependencies first — this layer is cached until requirements.txt changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# crawler.py uses flat imports (`from db import ...`), so the module files must
# sit beside it in WORKDIR. That is exactly what a flat copy here produces.
COPY . .

# Outbound-only network access, but it parses hostile HTML from 20k sites —
# don't hand that a root shell.
RUN useradd --create-home --uid 10001 quack
USER quack

CMD ["python", "crawler.py"]
