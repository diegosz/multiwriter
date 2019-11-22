package multiwriter

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"os"
	"testing"

	"github.com/spf13/afero"
)

var (
	testString     = "This is a not so long string i want to test"
	testByteString = []byte(testString)
)

func newHandler(t *testing.T, fileName string) *filehandler {
	semchan := make(chan token, 3)
	file, err := appFS.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("failed to open file %s: %s", fileName, err)
	}
	return &filehandler{
		done:    make(chan token),
		jobChan: make(chan *writeJob, 3),
		file:    file,
		semchan: semchan,
	}
}

func TestGzipWriter(t *testing.T) {
	appFS = afero.NewMemMapFs()
	filename := "/test.gz"
	wfh := New(2, 6, 2, 20, true)
	go wfh.Run()
	r := bytes.NewReader(testByteString)
	wfh.Write(filename, r)
	wfh.Stop()
	f, _ := appFS.Open(filename)
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	res, err := ioutil.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	if string(res) != string(testByteString) {
		t.Fatalf("gzip content differs from input string")
	}
}

func TestWriter(t *testing.T) {
	appFS = afero.NewMemMapFs()
	filename := "/test.txt"
	wfh := New(2, 6, 2, 20, false)
	go wfh.Run()
	r := bytes.NewReader(testByteString)
	wfh.Write(filename, r)
	wfh.Stop()
	f, _ := appFS.Open(filename)
	res, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(res) != testString {
		t.Fatalf("gzip content differs from input string")
	}
}

func TestGzipWriterFailNil(t *testing.T) {
	appFS = afero.NewMemMapFs()
	filename := "/test.gz"
	wfh := New(2, 6, 2, 20, true)
	go wfh.Run()
	Expected := "body cannot be nil"
	actual := wfh.Write(filename, nil)

	if actual.Error() != Expected {
		t.Errorf("Error actual = %v, and Expected = %v.", actual, Expected)
	}

	wfh.Stop()
	f, _ := appFS.Open(filename)
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	res, err := ioutil.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	if string(res) != "" {
		t.Fatalf("gzip content differs from input string")
	}
}

func TestWriterFailNil(t *testing.T) {
	appFS = afero.NewMemMapFs()
	filename := "/test.gz"
	wfh := New(2, 6, 2, 20, true)
	go wfh.Run()
	Expected := "body cannot be nil"
	actual := wfh.Write(filename, nil)
	if actual.Error() != Expected {
		t.Errorf("Error actual = %v, and Expected = %v.", actual, Expected)
	}
	wfh.Stop()
}

func TestGetWriter(t *testing.T) {
	appFS = afero.NewMemMapFs()
	fh := newHandler(t, "test.tmp")
	writer := fh.getWriter(true)
	written, err := writer.Write(testByteString)
	if err != nil {
		t.Error(err)
	}
	if written != len(testByteString) {
		t.Errorf("written bytes does not match, written %d, input %d", written, len(testByteString))
	}
}
