package util

import (
	"compress/gzip"
	"errors"
	"io"
	"sync"
)

var (
	gzipReaderPool = sync.Pool{
		New: func() any {
			return new(gzip.Reader)
			// return gzip.NewReader()
		},
	}

	gzipWriterPool = sync.Pool{
		New: func() any {
			w, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed)

			return w
		},
	}
)

func GzipStream(w io.Writer, r io.Reader) (int64, error) {
	gw, ok := gzipWriterPool.Get().(*gzip.Writer)
	if !ok {
		return 0, errors.New("gzip: new writer error")
	}
	gw.Reset(w)
	defer func() {
		gw.Close()
		gzipWriterPool.Put(gw)
	}()

	return io.Copy(gw, r)
}

func GunzipStream(w io.Writer, r io.Reader) (int64, error) {
	gr, ok := gzipReaderPool.Get().(*gzip.Reader)
	if !ok {
		return 0, errors.New("gzip: new reader error")
	}

	if err := gr.Reset(r); err != nil {
		return 0, err
	}
	defer func() {
		gr.Close()
		gzipReaderPool.Put(gr)
	}()

	return io.Copy(w, gr)
}
