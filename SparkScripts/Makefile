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
	--conf spark.executor.memory=4g \
	--conf spark.executor.cores=12 \
	--conf spark.driver.memory=4g \
	--conf spark.cores.max=36 \
	--conf spark.sql.shuffle.partitions=36 \
	./apps/$(app)
