// Copyright 2021 The Bitalosdb author(hustxrb@163.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bithash

import "github.com/zuoyebang/bitalosdb/internal/base"

type BithashWriter struct {
	b       *Bithash
	wr      *Writer
	compact bool
}

func (w *BithashWriter) Add(ikey base.InternalKey, value []byte) (FileNum, error) {
	var err error
	if w.wr == nil {
		return FileNum(0), err
	}

	if err = w.wr.Add(ikey, value); err != nil {
		return FileNum(0), err
	}

	fileNum := w.wr.fileNum
	w.b.stats.KeyTotal.Add(1)

	_ = w.maybeSplitTable()

	return fileNum, nil
}

func (w *BithashWriter) AddIkey(key *InternalKey, value []byte, khash uint32, fileNum FileNum) error {
	return w.wr.AddIkey(key.Clone(), value, khash, fileNum)
}

func (w *BithashWriter) maybeSplitTable() error {
	if !w.wr.isWriteFull() {
		return nil
	}

	if err := w.wr.Flush(); err != nil {
		return err
	}

	writer, err := NewTableWriter(w.b, w.compact)
	if err != nil {
		return err
	}

	closeWriter := w.wr
	w.wr = writer

	w.b.closeTableAsync(closeWriter, false)

	return nil
}

func (w *BithashWriter) Finish() error {
	if w.wr == nil {
		return nil
	}

	if err := w.wr.Flush(); err != nil {
		return err
	}

	if w.compact {
		return w.b.closeTable(w.wr, true)
	}

	if !w.wr.isWriteFull() {
		w.b.pushMutableWriters(w.wr)
	}

	return nil
}

func (w *BithashWriter) GetFileNum() FileNum {
	return w.wr.fileNum
}

func (w *BithashWriter) Remove() error {
	return w.wr.Remove()
}
