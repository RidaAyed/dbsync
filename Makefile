.PHONY: install build sh

install:
	docker-compose run --rm dbsync godep restore

build:
	docker-compose run --rm dbsync go build ./cmd/dbsync

sh:
	docker-compose run --rm dbsync /bin/sh

run:
	@docker-compose run --rm dbsync ./dbsync $(filter-out $@,$(MAKECMDGOALS))
%:
	@:
