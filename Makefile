SHELL := /bin/bash

.PHONY: up down logs ps restart test-sim stress-test federation-sim build

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

restart:
	docker compose restart

build:
	docker compose build

test-sim:
	python tests/integration_sim.py

federation-sim:
	python tests/federation_sim.py

stress-test:
	python tests/stress_test.py
