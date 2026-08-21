# Quack indexer — a batch job, not a service. Runs to completion and exits.
#   docker build -t quack-indexer ./indexer
#   docker run --rm -e MONGO_URI=mongodb://mongo:27017/ -e REDIS_HOST=redis quack-indexer
#
# Phase 1 holds the whole corpus's tokens in RAM — it is the memory ceiling of
# the system. Give this container real memory (see mem_limit in compose).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONIOENCODING=utf-8 \
    NLTK_DATA=/usr/share/nltk_data

WORKDIR /app

# Dependencies first — this layer is cached until requirements.txt changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# text_processing.py calls nltk.download() at import time with quiet=True, which
# fails *silently* when offline. Baking the corpora in means the container never
# needs the network for them, and a broken download breaks the build, not a
# three-hour indexing run. ("punkt" is unused dead weight — tokenization is a
# regex — but it is downloaded at import, so it is pre-baked to keep that a no-op.)
RUN python -m nltk.downloader -d "$NLTK_DATA" stopwords punkt

# indexer.py uses flat imports (`from db import ...`), so the module files must
# sit beside it in WORKDIR.
COPY . .

RUN useradd --create-home --uid 10001 quack
USER quack

CMD ["python", "indexer.py"]
