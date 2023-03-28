package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/RHEcosystemAppEng/SaaSi/s3tools/s3filemanager"
	"github.com/sirupsen/logrus"
)

func isValid(mode string, bucket string, folder string) bool {
	if mode != "download" && mode != "upload" {
		return false
	}
	if bucket == "" || folder == "" {
		return false
	}

	return true
}

func main() {
	log.Printf("Starting %s, %d", os.Args[0], os.Getpid())
	var mode, bucket, folder string
	var debug bool

	flag.StringVar(&mode, "m", "", "Run mode, either upload or download.")
	flag.StringVar(&bucket, "b", "", "Bucket name.")
	flag.StringVar(&folder, "f", "", "Folder name.")
	flag.BoolVar(&debug, "debug", false, "Debug the command by printing more information")
	flag.Parse()

	if !isValid(mode, bucket, folder) {
		flag.PrintDefaults()
		os.Exit(1)
	}

	sess, err := s3filemanager.ConnectWithEnvVariables()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Connected S3 session")

	start := time.Now()
	mode = strings.ToLower(mode)
	if mode == "download" {
		downloader := s3filemanager.NewS3BucketDownloader(bucket, folder, getLogger(debug))
		err = downloader.Run(sess)
	} else {
		uploader := s3filemanager.NewS3FolderUploader(bucket, folder, getLogger(debug))
		err = uploader.Run(sess)
	}

	if err != nil {
		log.Fatalf("Execution failed: %s", err)
	}

	elapsed := time.Since(start)
	log.Println("%s execution completed in %d", mode, elapsed)
}

func getLogger(debug bool) *logrus.Logger {
	log := logrus.New()
	if debug {
		log.SetLevel(logrus.DebugLevel)
	}
	return log
}
