.PHONY: build run

build:
	git submodule update --init --recursive
	docker build . -t dbsync

run:
	docker run --rm dbsync $(filter-out $@,$(MAKECMDGOALS))

