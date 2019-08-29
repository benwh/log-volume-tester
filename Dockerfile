FROM golang:1.12

WORKDIR /app
COPY . .

RUN go install -mod vendor -v ./...

ENTRYPOINT ["log-volume-tester"]
