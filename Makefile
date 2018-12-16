install:
	docker-compose run --rm --no-deps dbsync godep restore

build:
	docker-compose run --rm --no-deps dbsync go build ./cmd/dbsync
