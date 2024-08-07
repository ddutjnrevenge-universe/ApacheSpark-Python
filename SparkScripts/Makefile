build:
	docker-compose build

build-nc:
	docker-compose build --no-cache

build-progress:
	docker-compose build --no-cache --progress=plain

down:
	docker-compose down --volumes

run:
	make down && docker-compose up

run-scaled:
	make down && docker-compose up --scale spark-worker=3

run-d:
	make down && docker-compose up -d

stop:
	docker-compose stop

submit:
	docker exec da-spark-master spark-submit \
	--master spark://spark-master:7077 \
	--deploy-mode client \
	--executor-memory 2g \
	--driver-memory 2g \
	--total-executor-cores 18 \
	./apps/$(app)
