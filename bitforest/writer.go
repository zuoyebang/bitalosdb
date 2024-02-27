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

package bitforest

import (
	"sync"

	"github.com/zuoyebang/bitalosdb/internal/base"
)

type Writer struct {
	bf                *Bitforest
	commits           sync.WaitGroup
	commitSignals     []chan int
	commitRef         []int
	commitFinish      []chan bool
	commitClosedCh    []chan struct{}
	sem               chan struct{}
	currentWriteIndex []int
	unflushedKeys     [][2][]base.InternalKey
	unflushedValues   [][2][][]byte
}

func NewWriter(bf *Bitforest) *Writer {
	if bf.writer != nil {
		return bf.writer
	}

	w := &Writer{
		bf: bf,
	}

	treeNum := bf.treeNum
	w.commitSignals = make([]chan int, treeNum)
	w.commitRef = make([]int, treeNum)
	w.commitFinish = make([]chan bool, treeNum)
	w.commitClosedCh = make([]chan struct{}, treeNum)
	w.currentWriteIndex = make([]int, treeNum)
	w.unflushedKeys = make([][2][]base.InternalKey, treeNum)
	w.unflushedValues = make([][2][][]byte, treeNum)

	for i := 0; i < treeNum; i++ {
		for j := 0; j < 2; j++ {
			w.unflushedKeys[i][j] = make([]base.InternalKey, 0, bf.flushBufSize)
			w.unflushedValues[i][j] = make([][]byte, 0, bf.flushBufSize)
		}
		w.currentWriteIndex[i] = 0
		w.commitRef[i] = 0
		w.commitSignals[i] = make(chan int)
		w.commitFinish[i] = make(chan bool)
		w.commitClosedCh[i] = make(chan struct{})

		w.commits.Add(1)
		go w.runCommit(i)
	}

	return w
}

func (w *Writer) Start(memNum int) error {
	for i := 0; i < w.bf.treeNum; i++ {
		w.currentWriteIndex[i] = 0
		w.commitRef[i] = 0

		if err := w.bf.getBitree(i).MemFlushStart(); err != nil {
			return err
		}
	}

	semNum := 0
	treeNum := w.bf.treeNum
	if memNum == -1 {
		semNum = treeNum
	} else {
		if treeNum < 4 {
			semNum = memNum
		} else {
			if memNum <= treeNum/4 {
				semNum = treeNum / 2
			} else {
				semNum = treeNum
			}
		}
	}

	w.sem = make(chan struct{}, semNum)
	w.bf.opts.Logger.Infof("flush memtable start memNum(%d) treeNum(%d) semNum(%d)", memNum, treeNum, semNum)

	return nil
}

func (w *Writer) Set(key base.InternalKey, val []byte) error {
	uk := key.UserKey
	if len(uk) <= 0 {
		return nil
	}

	slotId := w.bf.getKeyBitreeIndex(uk)
	writeIndex := w.currentWriteIndex[slotId]
	w.unflushedKeys[slotId][writeIndex] = append(w.unflushedKeys[slotId][writeIndex], key.Clone())
	w.unflushedValues[slotId][writeIndex] = append(w.unflushedValues[slotId][writeIndex], val)

	if w.bf.cache != nil {
		cacheKeyHash := w.bf.cache.GetKeyHash(uk)
		switch key.Kind() {
		case base.InternalKeyKindSet:
			_ = w.bf.cache.Set(uk, val, cacheKeyHash)
		case base.InternalKeyKindDelete:
			_ = w.bf.cache.ExistAndDelete(uk, cacheKeyHash)
		}
	}

	if len(w.unflushedKeys[slotId][writeIndex]) >= w.bf.flushBufSize {
		if w.commitRef[slotId] > 0 {
			<-w.commitFinish[slotId]
			w.commitRef[slotId]--
		}

		w.commitRef[slotId]++

		w.commitSignals[slotId] <- writeIndex

		if writeIndex == 0 {
			w.currentWriteIndex[slotId] = 1
		} else {
			w.currentWriteIndex[slotId] = 0
		}
	}

	return nil
}

func (w *Writer) Finish() error {
	var err error

	for i := 0; i < w.bf.treeNum; i++ {
		if w.commitRef[i] > 0 {
			<-w.commitFinish[i]
			w.commitRef[i]--
		}

		bt := w.bf.getBitree(i)

		for j := 0; j < 2; j++ {
			if len(w.unflushedKeys[i][j]) <= 0 {
				continue
			}

			if err = bt.BatchUpdate(w.unflushedKeys[i][j], w.unflushedValues[i][j]); err != nil {
				w.bf.opts.Logger.Errorf("bitree(%d) BatchUpdate fail err:%s", i, err.Error())
			}
		}

		if err = bt.MemFlushFinish(); err != nil {
			w.bf.opts.Logger.Errorf("bitree(%d) MemFlushFinish fail err:%s", i, err.Error())
		}
	}

	w.release()
	return err
}

func (w *Writer) release() {
	for i := 0; i < w.bf.treeNum; i++ {
		for j := 0; j < 2; j++ {
			w.unflushedKeys[i][j] = w.unflushedKeys[i][j][:0]
			w.unflushedValues[i][j] = w.unflushedValues[i][j][:0]
		}
		w.currentWriteIndex[i] = -1
	}
	w.sem = nil
}

func (w *Writer) runCommit(i int) {
	defer w.commits.Done()

	for {
		select {
		case <-w.commitClosedCh[i]:
			return
		case queueId := <-w.commitSignals[i]:
			w.sem <- struct{}{}

			err := w.bf.getBitree(i).BatchUpdate(w.unflushedKeys[i][queueId], w.unflushedValues[i][queueId])
			if err != nil {
				w.bf.opts.Logger.Errorf("bitree(%d) BatchUpdate fail err:%s", i, err)
			}

			<-w.sem

			w.unflushedKeys[i][queueId] = w.unflushedKeys[i][queueId][:0]
			w.unflushedValues[i][queueId] = w.unflushedValues[i][queueId][:0]
			w.commitFinish[i] <- true
		}
	}
}
