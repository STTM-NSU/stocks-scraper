FROM golang:1.24.2-alpine3.21

ENV GOPATH=/
RUN go env -w GOCACHE=/.cache

COPY ./ ./

RUN --mount=type=cache,target=/.cache go build -mod=vendor -v -o stocks-scraper ./cmd/stocks-scraper

CMD ["./stocks-scraper"]
