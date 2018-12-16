install:
	docker-compose run --rm dbsync godep restore

build:
	docker-compose run --rm dbsync go build ./cmd/dbsync

sh:
	docker-compose run --rm dbsync /bin/sh
