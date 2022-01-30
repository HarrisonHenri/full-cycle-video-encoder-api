package services

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type VideoUpload struct {
	Paths        []string
	VideoPath    string
	OutputBucket string
	Errors       []string
}

func NewVideoUpload() *VideoUpload {
	return &VideoUpload{}
}

func (vu *VideoUpload) UploadObject(objectPath string, s *session.Session) error {

	// caminho/x/b/arquivo.mp4
	// split: caminho/x/b/
	// [0] caminho/x/b/
	// [1] arquivo.mp4
	path := strings.Split(objectPath, os.Getenv("LOCAL_STORAGE_PATH")+"/")

	f, err := os.Open(objectPath)
	if err != nil {
		return err
	}
	defer f.Close()

	fInfo, err := f.Stat()
	buffer := make([]byte, fInfo.Size())
	if err != nil {
		return err
	}
	f.Read(buffer)

	_, err = s3.New(s).PutObject(&s3.PutObjectInput{
		Bucket: aws.String(vu.OutputBucket),
		Key:    aws.String(path[1]),
		ACL:    aws.String("public-read"),
		Body:   bytes.NewReader(buffer),
	})

	if err != nil {
		return err
	}

	return nil

}

func (vu *VideoUpload) loadPaths() error {

	err := filepath.Walk(vu.VideoPath, func(path string, info os.FileInfo, err error) error {

		if !info.IsDir() {
			vu.Paths = append(vu.Paths, path)
		}
		return nil
	})

	if err != nil {
		return err
	}
	return nil
}

func (vu *VideoUpload) ProcessUpload(concurrency int, doneUpload chan string) error {

	in := make(chan int, runtime.NumCPU())
	returnChannel := make(chan string)

	err := vu.loadPaths()
	if err != nil {
		return err
	}

	s, err := getSession()
	if err != nil {
		return err
	}

	for process := 0; process < concurrency; process++ {
		go vu.uploadWorker(in, returnChannel, s)
	}

	go func() {
		for x := 0; x < len(vu.Paths); x++ {
			in <- x
		}
		close(in)
	}()

	for r := range returnChannel {
		if r != "" {
			doneUpload <- r
			break
		}
	}

	return nil

}

func (vu *VideoUpload) uploadWorker(in chan int, returnChan chan string, s *session.Session) {

	for x := range in {
		err := vu.UploadObject(vu.Paths[x], s)

		if err != nil {
			vu.Errors = append(vu.Errors, vu.Paths[x])
			log.Printf("error during the upload: %v. Error: %v", vu.Paths[x], err)
			returnChan <- err.Error()
		}

		returnChan <- ""
	}

	returnChan <- "upload completed"
}

func getSession() (*session.Session, error) {
	s, err := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("AWS_S3_REGION")),
		Credentials: credentials.NewStaticCredentials(
			os.Getenv("AWS_KEY_ID"),
			os.Getenv("AWS_PRIVATE_KEY"),
			""),
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}
