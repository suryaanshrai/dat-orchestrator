FROM python:3.10
WORKDIR /repo

COPY pyproject.toml .
COPY poetry.lock .
RUN pip install poetry

RUN poetry config virtualenvs.create false && poetry lock && poetry install --no-root
RUN pip install uvicorn[standard]
CMD celery -A src.Orchestrator.worker worker -Q dat-worker-q
