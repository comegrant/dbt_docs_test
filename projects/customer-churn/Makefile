.PHONY: help
help:
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

.PHONY: snapshot
snapshot:
	poetry run python -m customer_churn.snapshot

.PHONY: predict
predict:
	poetry run python -m customer_churn.predict

.PHONY: train
train:
	poetry run python -m customer_churn.train
