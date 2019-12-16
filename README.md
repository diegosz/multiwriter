# multiwriter

Append to the same files from many places and serialize the writes

Usage:

```golang
package main

import "github.com/roffe/multiwriter"
import "bytes"

func main() {
    mw := multiwriter.New(1, 16, 3, 128, true)
    defer mw.Stop()
    body := bytes.NewReader([]byte("My test string"))
    mw.Write("./test.txt", body)
}
```
