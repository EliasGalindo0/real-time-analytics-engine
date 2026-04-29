FROM golang:1.22-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN go mod tidy

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/engine ./cmd/engine

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/engine /engine
EXPOSE 8080
ENV ENGINE_ADDR=:8080
ENTRYPOINT ["/engine"]

