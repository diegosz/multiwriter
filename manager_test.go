package multiwriter

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/spf13/afero"
)

func TestNewManager(t *testing.T) {
	appFS = afero.NewMemMapFs()
	wfh := New(2, 6, 2, 20, true)
	go wfh.Run()
	wfh.Stop()
}

func TestManager(t *testing.T) {
	appFS = afero.NewMemMapFs()
	wfh := New(20, 6, 2, 20, true)
	jobs := []struct {
		filename string
		body     []byte
	}{
		{
			filename: "/data/test.gz",
			body:     []byte("first line\n"),
		},
		{
			filename: "/data/test2.gz",
			body:     []byte("test1233432 test rfefsdffs\n"),
		},
		{
			filename: "/data/test3.gz",
			body:     []byte("test1233432 test rfefsdffs\n"),
		},
		{
			filename: "/data/test4.gz",
			body:     []byte("test1233432 test rfefsdffs\n"),
		},
		{
			filename: "/data/test5.gz",
			body:     []byte("test1233432 test rfefsdffs\n"),
		},
		{
			filename: "/data/test6.gz",
			body:     []byte("third line\n"),
		},
		{
			filename: "/data/test7.gz",
			body:     []byte("test1233432 test rfefsdffs\n"),
		},
		{
			filename: "/data/test8.gz",
			body:     []byte("test1233432 test rfefsdffs\n"),
		},
		{
			filename: "/data/test9.gz",
			body:     []byte("test1233432 asdsasa tefafastrfefsdffs\n"),
		},
		{
			filename: "/data/test10.gz",
			body:     []byte("aaaaasdfgtest1233432 asdsasa tefafastrfefsdffs\n"),
		},
	}

	go wfh.Run()

	for _, job := range jobs {
		r := bytes.NewReader(job.body)
		wfh.Write(job.filename, r)
		respChan := make(chan error)
		wfh.jobChan <- &writeJob{
			filename: job.filename,
			body:     r,
			respChan: respChan,
			retry:    55,
		}
		if err := <-respChan; err != nil {
			t.Error(err)
		}
	}
	wfh.Stop()

	for _, job := range jobs {
		f, err := appFS.Open(job.filename)
		if err != nil {
			t.Error(err)
		}
		gz, err := gzip.NewReader(f)
		if err != nil {
			t.Error(err)
		}
		res, err := ioutil.ReadAll(gz)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(res, job.body) {
			t.Logf("expected %q", string(job.body))
			t.Logf("got %q", string(res))
			t.Error("written data does not match input")
		}
	}

}

func TestManagerNilJob(t *testing.T) {
	appFS = afero.NewMemMapFs()
	wfh := New(6, 6, 2, 20, true)
	go wfh.Run()
	wfh.jobChan <- nil
	wfh.Stop()
}
