FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/cometbft-archive ./cmd/cometbft-archive

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/cometbft-archive /usr/local/bin/cometbft-archive
ENTRYPOINT ["cometbft-archive"]
