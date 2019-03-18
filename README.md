[![Build Status](https://travis-ci.org/RidaAyed/dbsync.svg?branch=master)](https://travis-ci.org/RidaAyed/dbsync)

dbsync
======

This repo is a clone of
[https://bitbucket.org/modima/dbsync](https://bitbucket.org/modima/dbsync)

No code changes have been made (other than import paths). Instead I've added
support for running this within docker.

The original README for this repo can be found at `README.original.txt`

### Requirements

Ensure you have a recent version of docker installed on
your development machine


## Build

Build a executable

```sh
make [build]
```

## Run

You can execute `dbsync` in the command line of a docker instance by use the
`make run` command. Any flags/commands to `dbsync` put them after a
double-dash (`--`)

```sh
make run -- -ct -c 4
```

