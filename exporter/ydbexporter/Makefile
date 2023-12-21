include ../../Makefile.Common

local-run-example:
	cd ../../cmd/otelcontribcol && GOOS=linux go build -o ../../local/otelcontribcol
	cd example && docker compose up -d --build
