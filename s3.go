package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	ErrFileNotExist = errors.New("file does not exist")
)

type Uploader struct {
	cl *manager.Uploader

	Bucket string
}

func (upl *Uploader) Upload(ctx context.Context, r io.Reader, key string) error {
	_, err := upl.cl.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(upl.Bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	if err != nil {
		panic(err)
	}
	return nil
}

func (upl *Uploader) UploadFile(ctx context.Context, path string) error {
	key := filepath.Base(path)

	log.Printf("path:%s", path)
	log.Printf("key:%s", key)

	file, err := os.Open(path)
	if err != nil {
		return ErrFileNotExist
	}
	defer file.Close()

	// limit, _ := getFileSize(path)
	// bar := pb.Full.Start64(limit)
	// barReader := bar.NewProxyReader(file)
	// defer bar.Finish()

	if err := upl.Upload(ctx, file, key); err != nil {
		return fmt.Errorf("failed to Upload(%q):%w", path, err)
	}

	return nil
}

func getFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func NewUploader(bucket string) (*Uploader, error) {
	cl, err := NewClient()
	if err != nil {
		return nil, fmt.Errorf("failed to NewClient:%w", err)
	}
	up := &Uploader{
		cl:     cl,
		Bucket: bucket,
	}

	return up, nil
}

func NewClient() (*manager.Uploader, error) {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if service == s3.ServiceID && region == "ru-central1" {
			return aws.Endpoint{
				PartitionID:   "yc",
				URL:           "https://storage.yandexcloud.net",
				SigningRegion: "ru-central1",
			}, nil
		}
		return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		/*
			from env
		*/
		// config.WithRegion("ru-central1"),
		// config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")),
		config.WithEndpointResolverWithOptions(customResolver))

	if err != nil {
		return nil, fmt.Errorf("failed to LoadDefaultConfig:%w", err)
	}

	uploader := manager.NewUploader(s3.NewFromConfig(cfg), func(u *manager.Uploader) {
		u.Concurrency = 10
		u.PartSize = 100 * 1024 * 1024 // 100MB per part
		u.BufferProvider = manager.NewBufferedReadSeekerWriteToPool(100 * 1024 * 1024)
	})

	return uploader, nil
}
