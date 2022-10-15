package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"
	"sync"
)

func main() {
	source := flag.String("source", "", "The path to source file")
	flag.Parse()

	uploader, err := NewUploader("4kam-backup")
	if err != nil {
		log.Fatalf("Error NewUploader:%v", err)
	}

	files, err := os.ReadDir(*source)
	if err != nil {
		log.Fatalf("Error ReadDir(%s):%v", *source, err)
	}

	var wg sync.WaitGroup

	for _, file := range files {
		path := filepath.Join(*source, file.Name())
		wg.Add(1)
		go func() {
			defer wg.Done()

			log.Printf("%s uploading", path)
			if err := uploader.UploadFile(context.TODO(), path); err != nil {
				log.Printf("failed to UploadFile(%s):%v", path, err)
			}
			log.Printf("%s uploaded", path)

			if err := os.Remove(path); err != nil {
				log.Printf("failed to Remove(%s):%v", path, err)
			}
			log.Printf("%s removed", path)
		}()
	}

	wg.Wait()
}
