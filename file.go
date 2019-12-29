package urkeltrie

import "os"

type File struct {
	fd    *os.File
	dirty bool
}

func (f *File) Write(buf []byte) (int, error) {
	f.dirty = true
	return f.fd.Write(buf)
}

func (f *File) WriteAt(buf []byte, off int64) (int, error) {
	f.dirty = true
	return f.fd.WriteAt(buf, off)
}

func (f *File) Read(buf []byte) (int, error) {
	return f.fd.Read(buf)
}

func (f *File) ReadAt(buf []byte, off int64) (int, error) {
	return f.fd.ReadAt(buf, off)
}

func (f *File) Commit() error {
	if f.dirty {
		err := f.fd.Sync()
		if err != nil {
			return err
		}
	}
	f.dirty = false
	return nil
}
