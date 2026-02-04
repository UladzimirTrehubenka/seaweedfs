.PHONY: vendor test admin-generate admin-build admin-clean admin-dev admin-run admin-test admin-fmt admin-help

BINARY = weed
ADMIN_DIR = weed/admin

SOURCE_DIR = .
debug ?= 0

all: install

vendor:
	go mod vendor

lint_install:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.7.2

lint: vendor
	golangci-lint run --color always weed/...

lint_fix: vendor
	golangci-lint run --color always --fix weed/...

lint_clear:
	golangci-lint cache clean

build: vendor
	cd weed; go build -mod=vendor -ldflags="-s -w" -o ../bin/weed

bench_mini: build
	pkill weed || true
	nohup bin/weed mini -debug=$(debug) -dir=/tmp -master.volumeSizeLimitMB=1024 -volume.preStopSeconds=1 -s3.port=8000 -s3.allowDeleteBucketNotEmpty=false -s3.config=./docker/compose/s3.json >/tmp/weed.log 2>&1 &
	while ! nc -z localhost 8000 ; do sleep 1 ; done
	warp mixed --host=127.0.0.1:8000 --access-key=some_access_key1 --secret-key=some_secret_key1 --duration 5s --analyze.v
	pkill weed

install: admin-generate
	cd weed; go install

warp_install:
	go install github.com/minio/warp@v1.4.0

full_install: admin-generate
	cd weed; go install -tags "elastic gocdk sqlite ydb tarantool tikv rclone"

server: install
	weed -v 0 server -s3 -filer -filer.maxMB=64 -volume.max=0 -master.volumeSizeLimitMB=100 -volume.preStopSeconds=1 -s3.port=8000 -s3.allowDeleteBucketNotEmpty=true -s3.config=./docker/compose/s3.json -metricsPort=9324

benchmark: install warp_install
	pkill weed || true
	pkill warp || true
	weed server -debug=$(debug) -s3 -filer -volume.max=0 -master.volumeSizeLimitMB=100 -volume.preStopSeconds=1 -s3.port=8000 -s3.allowDeleteBucketNotEmpty=false -s3.config=./docker/compose/s3.json &
	warp client &
	while ! nc -z localhost 8000 ; do sleep 1 ; done
	warp mixed --host=127.0.0.1:8000 --access-key=some_access_key1 --secret-key=some_secret_key1 --autoterm
	pkill warp
	pkill weed

# curl -o profile "http://127.0.0.1:6060/debug/pprof/profile?debug=1"
benchmark_with_pprof: debug = 1
benchmark_with_pprof: benchmark

test: admin-generate
	cd weed; go test -tags "elastic gocdk sqlite ydb tarantool tikv rclone" -v ./...

test_race: admin-generate
	cd weed; go test -tags "elastic gocdk sqlite ydb tarantool tikv rclone" -race ./...

# Admin component targets
admin-generate:
	@echo "Generating admin component templates..."
	@cd $(ADMIN_DIR) && $(MAKE) generate

admin-build: admin-generate
	@echo "Building admin component..."
	@cd $(ADMIN_DIR) && $(MAKE) build

admin-clean:
	@echo "Cleaning admin component..."
	@cd $(ADMIN_DIR) && $(MAKE) clean

admin-dev:
	@echo "Starting admin development server..."
	@cd $(ADMIN_DIR) && $(MAKE) dev

admin-run:
	@echo "Running admin server..."
	@cd $(ADMIN_DIR) && $(MAKE) run

admin-test:
	@echo "Testing admin component..."
	@cd $(ADMIN_DIR) && $(MAKE) test

admin-fmt:
	@echo "Formatting admin component..."
	@cd $(ADMIN_DIR) && $(MAKE) fmt

admin-help:
	@echo "Admin component help..."
	@cd $(ADMIN_DIR) && $(MAKE) help
