dbsync
======

This repo is a clone of
[https://bitbucket.org/modima/dbsync](https://bitbucket.org/modima/dbsync)

No code changes have been made (other than import paths). Instead I've added
support for running this within docker.

The original README for this repo can be found at `README.original.txt`

### Requirements

Ensure you have a recent version of docker and docker-compose installed on
your development machine

## Install

Install dependencies via godep

```sh
make install
```

## Build

Build a executable 

```sh
make build
```

## Interactive Shell

To get an interactive shell within an docker instance so you can run `./dbsync
...` commands.


```sh
make sh
```
