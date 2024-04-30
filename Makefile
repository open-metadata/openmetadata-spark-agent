.PHONY: install_ci_dep
install_ci_dep:
	pip install openmetadata-ingestion
	pip install pyspark==3.5.0 pytest pymysql

.PHONY: start_om
start_om:
	docker compsoe up -f tests/resources/docker-compose.yaml -d
	python tests/resources/init_mysql.py
	metadata ingest -c tests/resources/mysql.yaml

.PHONY: build_dev
build_dev:
	mvn clean install -DskipTests