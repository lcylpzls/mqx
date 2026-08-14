package core

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"github.com/lcylpzls/errx"
)

// 文件操作注入接缝（测试注入失败分支用，生产恒为默认实现）。
var (
	storeMarshal  = json.Marshal
	storeUnmarshal = json.Unmarshal
	storeSeek     = func(f *os.File, off int64, whence int) (int64, error) { return f.Seek(off, whence) }
	storeWrite    = func(w *bufio.Writer, p []byte) (int, error) { return w.Write(p) }
	storeFlush    = func(w *bufio.Writer) error { return w.Flush() }
	storeTruncate = func(f *os.File, n int64) error { return f.Truncate(n) }
	storeSync     = func(f *os.File) error { return f.Sync() }
	storeNewScanner = func(r io.Reader) *bufio.Scanner {
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
		return sc
	}
)

// storeRecord 是文件存储的单行记录（追加日志格式）。
type storeRecord struct {
	Seq       int64             `json:"seq"`
	ID        string            `json:"id,omitempty"`
	Topic     string            `json:"topic,omitempty"`
	Key       string            `json:"key,omitempty"`
	Body      []byte            `json:"body,omitempty"`
	Attempt   int               `json:"attempt,omitempty"`
	EnqueueAt time.Time         `json:"enqueue_at,omitempty"`
	Attrs     map[string]string `json:"attrs,omitempty"`
	DeletedID string            `json:"deleted_id,omitempty"`
}

// fileStore 是基于追加日志的进程级持久化实现。
// 说明：写入为同步追加 + 缓冲刷盘（正常退出/重启不丢）；
// 非 WAL 级崩溃一致性，断电可能丢失最近写入。
type fileStore struct {
	mu   sync.Mutex
	path string
	f    *os.File
	seq  int64
	sync bool
}

// FileStoreOption 修改文件存储配置。
type FileStoreOption func(*fileStoreConfig)

// fileStoreConfig 是文件存储配置。
type fileStoreConfig struct {
	sync bool
}

// WithSync 开启同步模式：每次写入后 fsync，断电不丢已确认写入（吞吐较低）。
func WithSync() FileStoreOption {
	return func(c *fileStoreConfig) { c.sync = true }
}

// NewFileStore 创建文件存储（不存在则创建）。
func NewFileStore(path string, opts ...FileStoreOption) (*fileStore, error) {
	if path == "" {
		return nil, errInvalidConfig("存储文件路径不能为空")
	}
	cfg := fileStoreConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "打开存储文件失败")
	}
	s := &fileStore{path: path, f: f, sync: cfg.sync}
	if _, err := s.loadLocked(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

// SaveMessage 追加一条消息记录。
func (s *fileStore) SaveMessage(_ context.Context, msg *Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	rec := storeRecord{
		Seq:       s.seq,
		ID:        msg.ID,
		Topic:     msg.Topic,
		Key:       msg.Key,
		Body:      append([]byte(nil), msg.Body...),
		Attempt:   msg.Attempt,
		EnqueueAt: msg.EnqueueAt,
		Attrs:     cloneAttrs(msg.Attrs),
	}
	return s.appendLocked(rec)
}

// DeleteMessage 追加删除墓碑记录。
func (s *fileStore) DeleteMessage(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	return s.appendLocked(storeRecord{Seq: s.seq, DeletedID: id})
}

// LoadMessages 按保存顺序返回全部未删除消息。
func (s *fileStore) LoadMessages(_ context.Context) ([]*Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked()
}

// Close 关闭存储文件。
func (s *fileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}

// appendLocked 写入一条记录并刷盘（需持有锁）。
func (s *fileStore) appendLocked(rec storeRecord) error {
	data, err := storeMarshal(rec)
	if err != nil {
		return errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "存储记录序列化失败")
	}
	data = append(data, '\n')
	if _, err := storeSeek(s.f, 0, 2); err != nil {
		return errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "存储文件定位失败")
	}
	w := bufio.NewWriter(s.f)
	if _, err := storeWrite(w, data); err != nil {
		return errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "存储记录写入失败")
	}
	if err := storeFlush(w); err != nil {
		return errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "存储记录刷盘失败")
	}
	if s.sync {
		if err := storeSync(s.f); err != nil {
			return errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "存储记录同步失败")
		}
	}
	return nil
}

// loadLocked 重放日志：返回未删除消息并压实文件（需持有锁）。
func (s *fileStore) loadLocked() ([]*Message, error) {
	recs, err := s.scanRecords()
	if err != nil {
		return nil, err
	}
	deleted := make(map[string]struct{})
	var out []*Message
	for _, rec := range recs {
		if rec.Seq > s.seq {
			s.seq = rec.Seq
		}
		if rec.DeletedID != "" {
			deleted[rec.DeletedID] = struct{}{}
			continue
		}
		if _, ok := deleted[rec.ID]; ok {
			continue
		}
		out = append(out, &Message{
			ID:        rec.ID,
			Topic:     rec.Topic,
			Key:       rec.Key,
			Body:      rec.Body,
			Attempt:   rec.Attempt,
			EnqueueAt: rec.EnqueueAt,
			Attrs:     rec.Attrs,
		})
	}
	if len(deleted) > 0 {
		filtered := out[:0]
		for _, m := range out {
			if _, ok := deleted[m.ID]; !ok {
				filtered = append(filtered, m)
			}
		}
		out = filtered
		if err := s.compactLocked(deleted, recs); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// scanRecords 扫描并解析全部日志记录（需持有锁）。
func (s *fileStore) scanRecords() ([]storeRecord, error) {
	if _, err := storeSeek(s.f, 0, 0); err != nil {
		return nil, errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "存储文件定位失败")
	}
	sc := storeNewScanner(s.f)
	var live []storeRecord
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec storeRecord
		if err := storeUnmarshal(line, &rec); err != nil {
			return nil, errx.Wrap(err, errx.KindInvalid, CodeStoreFailed, "存储记录解析失败")
		}
		live = append(live, rec)
	}
	if err := sc.Err(); err != nil {
		return nil, errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "存储文件读取失败")
	}
	return live, nil
}

// compactLocked 重写文件，移除墓碑与已删除消息记录（需持有锁）。
func (s *fileStore) compactLocked(deleted map[string]struct{}, recs []storeRecord) error {
	var live []storeRecord
	for _, rec := range recs {
		if rec.DeletedID != "" {
			continue
		}
		if _, ok := deleted[rec.ID]; ok {
			continue
		}
		live = append(live, rec)
	}
	if err := storeTruncate(s.f, 0); err != nil {
		return errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "存储文件截断失败")
	}
	if _, err := storeSeek(s.f, 0, 0); err != nil {
		return errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "存储文件定位失败")
	}
	w := bufio.NewWriter(s.f)
	for _, rec := range live {
		data, err := storeMarshal(rec)
		if err != nil {
			return errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "存储记录序列化失败")
		}
		if _, err := storeWrite(w, append(data, '\n')); err != nil {
			return errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "存储记录写入失败")
		}
	}
	if err := storeFlush(w); err != nil {
		return errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "存储记录刷盘失败")
	}
	if s.sync {
		if err := storeSync(s.f); err != nil {
			return errx.Wrap(err, errx.KindUnavailable, CodeStoreFailed, "存储记录同步失败")
		}
	}
	return nil
}
