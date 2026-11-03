# Build from the repository root:  docker build -f tools/bankman-bootstrap/Dockerfile -t bankman-bootstrap .
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ledgertrack-import ./cmd/ledgertrack-import \
 && CGO_ENABLED=0 go build -o /out/bankman-reconcile ./tools/bankman-reconcile

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 mariadb-client curl ca-certificates \
 && (command -v mysql >/dev/null || ln -s "$(command -v mariadb)" /usr/local/bin/mysql) \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ledgertrack-import /out/bankman-reconcile /usr/local/bin/
COPY tools/bankman-migration/export_bankman.py tools/bankman-migration/coa.py /opt/migration/
COPY tools/bankman-bootstrap/run.sh /usr/local/bin/bankman-bootstrap
ENTRYPOINT ["/usr/local/bin/bankman-bootstrap"]
