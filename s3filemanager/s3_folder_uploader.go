package s3filemanager

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/sirupsen/logrus"
)

type S3FolderUploader struct {
	bucket string
	folder string
	svc    *s3.S3
	logger *logrus.Logger
}

func NewS3FolderUploader(bucket string, folder string, logger *logrus.Logger) *S3FolderUploader {
	return &S3FolderUploader{bucket: bucket, folder: folder, logger: logger}
}

func (s3fu *S3FolderUploader) Run(sess *session.Session) error {
	s3fu.svc = s3.New(sess)
	targetBucket, err := s3fu.svc.CreateBucket(&s3.CreateBucketInput{
		Bucket:                    &s3fu.bucket,
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{},
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeBucketAlreadyOwnedByYou {
			s3fu.logger.Infof("Bucket %s already exists", s3fu.bucket)
		} else {
			s3fu.logger.Errorf("Cannot create bucket %s", err)
			return err
		}
	} else {
		s3fu.logger.Infof("Bucket %s created", targetBucket.GoString())
	}

	err = s3fu.uploadFolder(sess)
	if err != nil {
		s3fu.logger.Infof("Upload failed: %s", err)
	}
	return err
}

type lazyFileReader struct {
	file   string
	f      fs.File
	logger *logrus.Logger
}

func (r *lazyFileReader) Read(p []byte) (n int, err error) {
	r.logger.Debugf("Now reading %s", r.file)
	if r.f == nil {
		r.f, err = os.Open(r.file)
		if err != nil {
			return 0, fmt.Errorf("cannot open %s: %s", r.file, err)
		}
	}
	n, err = r.f.Read(p)
	if n == 0 {
		r.logger.Debugf("Now closing %s", r.file)
		defer r.f.Close()
	}
	r.logger.Debugf("Open files: %d", countOpenFiles())
	return n, err
}

func (s3fu *S3FolderUploader) uploadFolder(sess *session.Session) error {
	var objects []s3manager.BatchUploadObject
	s3fu.logger.Infof("Start WalkDir")
	err := filepath.WalkDir(s3fu.folder, func(path string, d fs.DirEntry, err error) error {
		if !d.IsDir() {
			objects = append(objects, s3manager.BatchUploadObject{
				Object: &s3manager.UploadInput{
					Key:    aws.String(path),
					Bucket: aws.String(s3fu.bucket),
					Body:   &lazyFileReader{file: path, logger: s3fu.logger},
				},
			})
		}
		return nil
	})
	if err != nil {
		s3fu.logger.Errorf("Cannot iterate folder %s: %s", s3fu.folder, err)
		return err
	}

	s3fu.logger.Infof("Start UploadWithIterator with %d objects", len(objects))
	iter := &s3manager.UploadObjectsIterator{Objects: objects}
	uploader := s3manager.NewUploader(sess)
	if err := uploader.UploadWithIterator(aws.BackgroundContext(), iter); err != nil {
		return err
	}

	s3fu.logger.Infof("Success3fully uploaded %d files from folder %s to bucket %s", len(objects), s3fu.folder, s3fu.bucket)
	return nil
}
