.PHONY: up-db
up-db:
	docker compose -f docker-compose.db.yml up -d

.PHONY: i
i:
	poetry install

.PHONY: sequelize
sequelize:
	poetry run python sequelize.

.PHONY: run
run:
	poetry run uvicorn api:app --reload
