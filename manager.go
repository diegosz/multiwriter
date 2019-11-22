package multiwriter

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/spf13/afero"
)

var appFS = afero.NewOsFs()

var (
	// DefaultTimeout is the default tiemout value
	DefaultTimeout  = time.Duration(15 * time.Second)
	errTimeOut      = fmt.Errorf("took to long to put write job, timeout %s", DefaultTimeout.String())
	errWriteTimeout = fmt.Errorf("write response timeout %s", DefaultTimeout.String())
)

type token struct{}

// Manager ...
type Manager struct {
	gzipOutput bool
	jobChan    chan *writeJob
	lru        *lru.Cache
	semchan    chan token
	wg         *sync.WaitGroup
	writeDepth int
}

// New returns a new multiwriter
func New(jobbQlenght int, maxConcurrentWrites int, writeDepth int, lruSize int, gzipOutput bool) *Manager {
	var wg sync.WaitGroup
	lru, err := lru.NewWithEvict(lruSize, evictFunc)
	if err != nil {
		log.Fatal(err)
	}
	m := &Manager{
		gzipOutput: gzipOutput,
		jobChan:    make(chan *writeJob, jobbQlenght),
		lru:        lru,
		semchan:    make(chan token, maxConcurrentWrites),
		wg:         &wg,
		writeDepth: writeDepth,
	}
	return m
}

func evictFunc(key interface{}, value interface{}) {
	fh, ok := value.(*filehandler)
	if !ok {
		log.Fatal("not valid filehandler")
	}
	close(fh.jobChan)
	<-fh.done
}

// Stop ...
func (m *Manager) Stop() {
	close(m.jobChan)
	log.Println("wait for all filehandles to flush")
	m.wg.Wait()
	log.Println("multiwriter exited")
}

// Run starts the manager
func (m *Manager) Run() {
	defer m.lru.Purge()
	for job := range m.jobChan {
		if job == nil {
			break
		}
		if job.retry > 1000 {
			job.respChan <- fmt.Errorf("to many retries to write to %s", job.filename)
			close(job.respChan)
			continue
		}
		m.handle(job)
	}
}

func (m *Manager) handle(job *writeJob) {
	var v interface{}
	v, ok := m.lru.Get(job.filename)
	if !ok {
		handler := m.newFileHandler(job)
		m.lru.Add(job.filename, handler)
		v = handler
	}

	if job.retry > 50 {
		log.Printf("queue write for %s retry %d\n", job.filename, job.retry)
	}
	job.retry++
	select {
	case v.(*filehandler).jobChan <- job:
	case <-time.After((time.Duration(job.retry) * 10) * time.Millisecond):
		select {
		case m.jobChan <- job:
		case <-time.After(60 * time.Second):
			log.Fatal("multifilewriter has become deadlocked")
		}
	}
}

func (m *Manager) newFileHandler(job *writeJob) *filehandler {
	if err := createPath(job.filename); err != nil {
		log.Fatal("failed to create directory", err)
	}
	file, err := appFS.OpenFile(job.filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)

	if err != nil {
		log.Fatalf("failed to open file %s: %s", job.filename, err)
	}
	handler := &filehandler{
		done:    make(chan token),
		jobChan: make(chan *writeJob, m.writeDepth),
		file:    file,
		semchan: m.semchan,
	}
	m.wg.Add(1)
	go handler.run(m.wg, m.gzipOutput)
	return handler
}

func (m *Manager) retry(job *writeJob) {
	defer m.wg.Done()
	select {
	case m.jobChan <- job:
	case <-time.After(DefaultTimeout):
		log.Fatalf("took to long to re-queue write job, %s", DefaultTimeout)
	}
}

// Write appends to the given filename.
// Folders & file will be created if missing in fs
func (m *Manager) Write(filename string, body io.Reader) error {
	respChan := make(chan error)
	select {
	case m.jobChan <- &writeJob{
		filename: filename,
		body:     body,
		respChan: respChan,
	}:
	case <-time.After(15 * time.Second):
		log.Fatal("took way to long to put job in work queue, deadlocked?")
	}
	return <-respChan
}
