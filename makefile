# Use bash on all platforms
# Windows users: Requires Git Bash (comes with Git for Windows: https://git-scm.com/download/win)
# Add C:\Program Files\Git\bin to your PATH, or run make from Git Bash terminal
ifeq ($(OS),Windows_NT)
    SHELL := C:/Program Files/Git/bin/bash.exe
    .SHELLFLAGS := -ec
else
    SHELL := /bin/bash
endif

#setup:
#	@echo "Setting up the environment..."

#build: build-frontend build-backend

build:
	@echo "Building the project..."
	go build -o bin/webdav_sync_tool
	@echo "✓ Build complete."

run:
	@echo "Run the project..."
	go run .