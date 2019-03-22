FROM golang:alpine as build

RUN apk update && apk upgrade && \
    apk add --no-cache git

RUN go get github.com/tools/godep

COPY . /go/src/github.com/ridaayed/dbsync
WORKDIR /go/src/github.com/ridaayed/dbsync

RUN godep restore -v
RUN go build -v -o /dbsync ./cmd/dbsync

FROM alpine
RUN apk update && apk upgrade && \
    apk add --no-cache ca-certificates

COPY --from=build /dbsync /bin/dbsync
ENTRYPOINT [ "/bin/dbsync" ]
CMD [ "--help" ]

