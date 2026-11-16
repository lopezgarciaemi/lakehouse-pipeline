.PHONY: up down build logs test lint local-run clean

up:
	cp -n .env.example .env || true
	docker compose up -d --build

down:
	docker compose down

down-v:
	docker compose down -v

logs:
	docker compose logs -f airflow-scheduler

test:
	python -m pytest

lint:
	python -m ruff check .

local-run:
	python -m src.pipelines.local_runner --mock-api

clean:
	rm -rf data/ .pytest_cache/ .ruff_cache/ dbt/target/ dbt/logs/
