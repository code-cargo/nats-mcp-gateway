VERSION ?= develop
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

ifdef CI_VERSION
VERSION := $(CI_VERSION)
endif

# Common directories
bin_dir := bin

# Windows executables need the .exe suffix; `go build -o <name>` does NOT add
# it automatically. Derive it from the target GOOS (which go build reads from
# the environment), so cross-builds and local builds stay consistent.
EXE := $(if $(filter windows,$(GOOS)),.exe,)
BINARY := natsmcp$(EXE)

# Common build flags. Deferred (=) so $(VERSION) resolves at recipe time,
# after any CI_VERSION override above — with := it would bake in "develop".
LDFLAGS = -w -s -X main.version=$(VERSION)

# Define color codes
GREEN := \033[32m
RED := \033[31m
RESET := \033[0m

.PHONY: all build ci install-tools vet fmt fmt-check test test-ci tidy clean demo

.DEFAULT_GOAL := build

# Define function to check for required tools
define check_tool
	@if ! command -v $(1) >/dev/null 2>&1; then \
		printf "${RED}$(1) not found${RESET}\n"; \
		printf "${RED}To install required tools, run: make install-tools${RESET}\n"; \
		exit 1; \
	fi
endef

all: fmt vet test build
ci: install-tools fmt-check vet build test-ci

install-tools:
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install -v github.com/incu6us/goimports-reviser/v3@latest
	go install mvdan.cc/gofumpt@latest
	@printf "${GREEN}Ensure tools directory is on PATH - default: $${HOME}/go/bin${RESET}\n"

$(bin_dir):
	mkdir -p $@

build: | $(bin_dir)
	@printf "${GREEN}Building $(BINARY)...${RESET}\n"
	@CGO_ENABLED=0 go build \
		-ldflags="$(LDFLAGS)" \
		-o "$(bin_dir)/$(BINARY)" . || \
		(printf "${RED}Build failed for $(BINARY)${RESET}\n" && exit 1)

test:
	@printf "${GREEN}Running tests...${RESET}\n"
	go test ./...

test-ci:
	@printf "${GREEN}Running CI tests...${RESET}\n"
	go run gotest.tools/gotestsum@latest --junitfile test-results.xml --format testdox -- ./...

vet:
	$(call check_tool,staticcheck)
	@printf "${GREEN}Running staticcheck...${RESET}\n"
	staticcheck ./...

fmt:
	$(call check_tool,goimports-reviser)
	@printf "${GREEN}Running goimports-reviser...${RESET}\n"
	goimports-reviser -rm-unused -set-alias -format ./...
	$(call check_tool,gofumpt)
	@printf "${GREEN}Running gofumpt...${RESET}\n"
	gofumpt -w .

fmt-check:
	$(call check_tool,goimports-reviser)
	@printf "${GREEN}Checking formatting with goimports-reviser...${RESET}\n"
	@if [ -n "$$(goimports-reviser -rm-unused -set-alias -format -set-exit-status -list-diff -output write ./...)" ]; then \
		printf "${RED}goimports-reviser found formatting issues${RESET}\n"; \
		exit 1; \
	fi
	$(call check_tool,gofumpt)
	@printf "${GREEN}Checking formatting with gofumpt...${RESET}\n"
	@if [ -n "$$(gofumpt -l .)" ]; then \
		printf "${RED}gofumpt found formatting issues${RESET}\n"; \
		exit 1; \
	fi

tidy:
	@printf "${GREEN}Running go mod tidy...${RESET}\n"
	go mod tidy

demo: build
	@printf "${GREEN}Starting demo (see demo/README.md)...${RESET}\n"
	cd demo && ./run.sh

clean:
	@printf "${GREEN}Cleaning build artifacts...${RESET}\n"
	rm -rf $(bin_dir)
