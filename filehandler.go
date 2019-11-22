package multiwriter

import (
	"compress/gzip"
	"errors"
	"io"
	"log"
	"path/filepath"
	"sync"
)

type filehandler struct {
	done    chan token
	jobChan chan *writeJob
	file    file
	semchan chan token
	sync.RWMutex
}

type writeJob struct {
	body     io.Reader
	filename string
	respChan chan error
	retry    int
}

type file interface {
	io.Closer
	io.Reader
	io.Writer
	Sync() error
	Name() string
}

func createPath(filename string) error {
	return appFS.MkdirAll(filepath.Dir(filename), 0700)
}

func (fh *filehandler) getWriter(gzipEnabled bool) io.WriteCloser {
	if gzipEnabled {
		gz, _ := gzip.NewWriterLevel(fh.file, gzip.BestSpeed)
		return gz
	}
	return fh.file
}

func (fh *filehandler) run(wg *sync.WaitGroup, gzipOutput bool) {
	defer func() {
		wg.Done()
		close(fh.done)
	}()
	writer := fh.getWriter(gzipOutput)
	for job := range fh.jobChan {
		if job.body == nil {
			job.respChan <- errors.New("body cannot be nil")
			close(job.respChan)
			break
		}
		fh.semchan <- token{}
		_, err := io.Copy(writer, job.body)
		if err != nil {
			log.Fatal("failed io.copy ", err)
		}
		if err := fh.file.Sync(); err != nil {
			log.Fatal("failed to sync filehandle ", err)
		}
		close(job.respChan)
		<-fh.semchan
	}
	if gzipOutput {
		if err := writer.Close(); err != nil {
			log.Fatal("failed to close gzip writer ", err)
		}
	}
	if err := fh.file.Sync(); err != nil {
		log.Fatal("failed to sync filehandle ", err)
	}
	if err := fh.file.Close(); err != nil {
		log.Fatal("failed to close filehandle ", err)
	}
}
