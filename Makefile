.PHONY: all build docker

GIT_TAG=$(shell git describe --abbrev=0 --tags)
VERSION=$(GIT_TAG:v%=%)

build:
	CGO_ENABLED=0 GOOS=linux   GOARCH=amd64 go build -o bin/linux/archive_${VERSION}_linux_amd64         .
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o bin/windows/archive_${VERSION}_windows_amd64.exe .
	CGO_ENABLED=0 GOOS=darwin  GOARCH=arm64 go build -o bin/darwin/archive_${VERSION}_darwin_arm64    	 .
