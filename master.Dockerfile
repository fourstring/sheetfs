##
## Build
##

FROM golang:1.16-buster AS build

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY ./master ./master
COPY ./protocol ./protocol

RUN cd master && go build

WORKDIR /app/master
RUN chmod +x ./docker-entrypoint.sh

CMD ["./docker-entrypoint.sh"]