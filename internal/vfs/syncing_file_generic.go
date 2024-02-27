//go:build !linux || arm
// +build !linux arm

package vfs

func (f *syncingFile) init() {
	f.syncTo = f.syncToGeneric
}

func (f *syncingFile) syncToGeneric(_ int64) error {
	return f.Sync()
}
