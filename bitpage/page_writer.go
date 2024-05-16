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

package bitpage

type PageWriter struct {
	p        *page
	Sentinel []byte
}

func (w *PageWriter) Set(key internalKey, value []byte) error {
	return w.p.set(key, value)
}

func (w *PageWriter) UpdateMetaTimestamp() {
	w.p.bp.meta.updatePagemetaTimestamp(w.p.pn)
}

func (w *PageWriter) FlushFinish() error {
	return w.p.memFlushFinish()
}

func (w *PageWriter) MaybePageFlush(size uint64) bool {
	return w.p.maybeScheduleFlush(size, false)
}
