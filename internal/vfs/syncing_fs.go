package vfs

// syncingFS wraps a vfs.FS with one that wraps newly created files with
// vfs.NewSyncingFile.
type syncingFS struct {
	FS

	syncOpts SyncingFileOptions
}

func WithSyncingFS(fs FS, opts SyncingFileOptions) FS {
	return syncingFS{
		FS:       fs,
		syncOpts: opts,
	}
}

func (fs syncingFS) Create(name string) (File, error) {
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return NewSyncingFile(f, fs.syncOpts), nil
}
