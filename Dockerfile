FROM python:3.10
WORKDIR /repo

RUN apt-get update && apt-get install -y wget unzip && \
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb

COPY pyproject.toml .
COPY poetry.lock .
RUN pip install poetry

RUN poetry config virtualenvs.create false && poetry lock && poetry install --no-root
RUN pip install uvicorn[standard]
CMD celery -A src.Orchestrator.worker worker -Q dat-worker-q
