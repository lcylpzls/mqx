package core

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcylpzls/errx"
	"github.com/lcylpzls/testx"
)

// fakeStore 是可注入失败的测试存储。
type fakeStore struct {
	mu        sync.Mutex
	saved     []*Message
	deleted   []string
	saveErr   error
	deleteErr error
	loadErr   error
	loaded    []*Message
}

func (f *fakeStore) SaveMessage(_ context.Context, msg *Message) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	f.mu.Lock()
	f.saved = append(f.saved, msg)
	f.mu.Unlock()
	return nil
}

func (f *fakeStore) DeleteMessage(_ context.Context, id string) error {
	f.mu.Lock()
	f.deleted = append(f.deleted, id)
	f.mu.Unlock()
	if f.deleteErr != nil {
		return f.deleteErr
	}
	return nil
}

func (f *fakeStore) LoadMessages(context.Context) ([]*Message, error) {
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	return f.loaded, nil
}

// TestFileStoreBasic 覆盖文件存储保存/删除/恢复/重开。
func TestFileStoreBasic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mq.jsonl")
	fs, err := NewFileStore(path)
	testx.RequireNoError(t, err)
	ctx := context.Background()
	msgs := []*Message{
		{ID: "a", Topic: "t", Key: "k1", Body: []byte("1"), Attempt: 1,
			EnqueueAt: time.Unix(1, 0), Attrs: map[string]string{"x": "y"}},
		{ID: "b", Topic: "t", Key: "k1", Body: []byte("2"), Attempt: 2,
			EnqueueAt: time.Unix(2, 0)},
	}
	for _, m := range msgs {
		testx.RequireNoError(t, fs.SaveMessage(ctx, m))
	}
	loaded, err := fs.LoadMessages(ctx)
	testx.RequireNoError(t, err)
	testx.RequireEqual(t, len(loaded), 2)
	testx.RequireEqual(t, loaded[0].ID, "a")
	testx.RequireEqual(t, loaded[0].Attrs["x"], "y")
	testx.RequireEqual(t, loaded[1].Attempt, 2)

	testx.RequireNoError(t, fs.DeleteMessage(ctx, "a"))
	loaded, err = fs.LoadMessages(ctx)
	testx.RequireNoError(t, err)
	testx.RequireEqual(t, len(loaded), 1)
	testx.RequireEqual(t, loaded[0].ID, "b")
	testx.RequireNoError(t, fs.Close())

	// 重开：墓碑已压实，仅剩 b；序列号继续增长。
	fs2, err := NewFileStore(path)
	testx.RequireNoError(t, err)
	defer func() { _ = fs2.Close() }()
	loaded, err = fs2.LoadMessages(ctx)
	testx.RequireNoError(t, err)
	testx.RequireEqual(t, len(loaded), 1)
	testx.RequireEqual(t, loaded[0].ID, "b")
	testx.RequireNoError(t, fs2.SaveMessage(ctx, &Message{ID: "c", Topic: "t", Key: "k2"}))
	loaded, err = fs2.LoadMessages(ctx)
	testx.RequireNoError(t, err)
	testx.RequireEqual(t, len(loaded), 2)
	testx.RequireEqual(t, loaded[1].ID, "c")
}

// TestFileStoreErrors 覆盖文件存储错误分支。
func TestFileStoreErrors(t *testing.T) {
	if _, err := NewFileStore(""); !errx.Is(err, CodeInvalidConfig) {
		t.Fatalf("空路径应报错：%v", err)
	}
	dir := t.TempDir()
	if _, err := NewFileStore(dir); err == nil {
		t.Fatal("目录路径应报错")
	}
	path := filepath.Join(dir, "bad.jsonl")
	testx.RequireNoError(t, os.WriteFile(path, []byte("not-json\n"), 0o600))
	if _, err := NewFileStore(path); !errx.Is(err, CodeStoreFailed) {
		t.Fatalf("坏记录应报错：%v", err)
	}
	good := filepath.Join(dir, "good.jsonl")
	fs, err := NewFileStore(good)
	testx.RequireNoError(t, err)
	ctx := context.Background()
	testx.RequireNoError(t, fs.SaveMessage(ctx, &Message{ID: "a", Topic: "t", Key: "k"}))
	testx.RequireNoError(t, fs.Close())
	// 关闭后写入应报存储错误。
	if err := fs.SaveMessage(ctx, &Message{ID: "b", Topic: "t", Key: "k"}); !errx.Is(err, CodeStoreFailed) {
		t.Fatalf("关闭后写入应报错：%v", err)
	}
}

// TestMQWithStorePersistRecover 覆盖落盘、恢复与确认后删除。
func TestMQWithStorePersistRecover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mq.jsonl")
	store1, err := NewFileStore(path)
	testx.RequireNoError(t, err)
	m1, err := New(WithStore(store1))
	testx.RequireNoError(t, err)
	cfg := smallTopic()
	cfg.QueueSize = 32
	_, err = m1.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m1.Produce(context.Background(), "orders", "k1", []byte("1")))
	testx.RequireNoError(t, m1.Produce(context.Background(), "orders", "k1", []byte("2")))
	testx.RequireNoError(t, m1.Produce(context.Background(), "orders", "k2", []byte("3")))
	testx.RequireNoError(t, m1.Shutdown(context.Background()))
	testx.RequireNoError(t, store1.Close())

	store2, err := NewFileStore(path)
	testx.RequireNoError(t, err)
	defer func() { _ = store2.Close() }()
	m2, err := New(WithStore(store2))
	testx.RequireNoError(t, err)
	defer func() { _ = m2.Shutdown(context.Background()) }()
	_, err = m2.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m2.Recover(context.Background()))

	var mu sync.Mutex
	received := map[string][]string{}
	_, err = m2.Subscribe(context.Background(), "orders", "g", 1, func(_ context.Context, msg *Message) error {
		mu.Lock()
		received[msg.Key] = append(received[msg.Key], string(msg.Body))
		mu.Unlock()
		return nil
	})
	testx.RequireNoError(t, err)
	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received["k1"]) == 2 && len(received["k2"]) == 1
	})
	mu.Lock()
	// k1 两条顺序保持（跨分区不保证全序）。
	testx.RequireEqual(t, received["k1"][0], "1")
	testx.RequireEqual(t, received["k1"][1], "2")
	testx.RequireEqual(t, received["k2"][0], "3")
	mu.Unlock()
	// 全部确认后消息从存储删除：重开恢复应为空。
	time.Sleep(50 * time.Millisecond)
	store3, err := NewFileStore(path)
	testx.RequireNoError(t, err)
	loaded, err := store3.LoadMessages(context.Background())
	testx.RequireNoError(t, err)
	testx.RequireEqual(t, len(loaded), 0)
	testx.RequireNoError(t, store3.Close())
}

// TestProduceStoreFailure 覆盖落盘失败。
func TestProduceStoreFailure(t *testing.T) {
	fs := &fakeStore{saveErr: errx.New(errx.KindUnavailable, CodeStoreFailed, "落盘失败")}
	m, err := New(WithStore(fs))
	testx.RequireNoError(t, err)
	defer func() { _ = m.Shutdown(context.Background()) }()
	_, err = m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	if err := m.Produce(context.Background(), "orders", "k", nil); !errx.Is(err, CodeStoreFailed) {
		t.Fatalf("落盘失败应报错：%v", err)
	}
	if err := m.ProduceBatch(context.Background(), "orders", []ProduceItem{{Key: "k"}}); !errx.Is(err, CodeStoreFailed) {
		t.Fatalf("批量落盘失败应报错：%v", err)
	}
}

// TestDeleteFailureTolerated 覆盖确认删除失败仍继续消费。
func TestDeleteFailureTolerated(t *testing.T) {
	fs := &fakeStore{deleteErr: errx.New(errx.KindUnavailable, CodeStoreFailed, "删除失败")}
	m, err := New(WithStore(fs))
	testx.RequireNoError(t, err)
	defer func() { _ = m.Shutdown(context.Background()) }()
	_, err = m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	var consumed atomic.Int32
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		consumed.Add(1)
		return nil
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	waitFor(t, 2*time.Second, func() bool { return consumed.Load() == 1 })
	fs.mu.Lock()
	testx.RequireEqual(t, len(fs.deleted), 1)
	fs.mu.Unlock()
}

// TestRecoverErrors 覆盖恢复错误分支。
func TestRecoverErrors(t *testing.T) {
	m, err := New()
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Recover(context.Background()))

	loadErr := &fakeStore{loadErr: errx.New(errx.KindUnavailable, CodeStoreFailed, "加载失败")}
	m2, err := New(WithStore(loadErr))
	testx.RequireNoError(t, err)
	if err := m2.Recover(context.Background()); !errx.Is(err, CodeStoreFailed) {
		t.Fatalf("加载失败应报错：%v", err)
	}

	unknown := &fakeStore{loaded: []*Message{{ID: "a", Topic: "missing", Key: "k"}}}
	m3, err := New(WithStore(unknown))
	testx.RequireNoError(t, err)
	defer func() { _ = m3.Shutdown(context.Background()) }()
	if err := m3.Recover(context.Background()); !errx.Is(err, CodeStoreFailed) {
		t.Fatalf("未知主题应报错：%v", err)
	}
}

// TestRecoverQueueFull 覆盖恢复入队失败分支。
func TestRecoverQueueFull(t *testing.T) {
	fs := &fakeStore{loaded: []*Message{
		{ID: "a", Topic: "t", Key: "k"},
		{ID: "b", Topic: "t", Key: "k"},
	}}
	m, err := New(WithStore(fs))
	testx.RequireNoError(t, err)
	defer func() { _ = m.Shutdown(context.Background()) }()
	cfg := smallTopic()
	cfg.QueueFullPolicy = QueueFullDrop
	_, err = m.CreateTopic("t", cfg)
	testx.RequireNoError(t, err)
	if err := m.Recover(context.Background()); !errx.Is(err, CodeStoreFailed) {
		t.Fatalf("恢复入队失败应报错：%v", err)
	}
}

// TestDLQPersistRecover 覆盖死信落盘、恢复与删除。
func TestDLQPersistRecover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mq.jsonl")
	store1, err := NewFileStore(path)
	testx.RequireNoError(t, err)
	m1, err := New(WithStore(store1))
	testx.RequireNoError(t, err)
	cfg := smallTopic()
	cfg.QueueSize = 32
	_, err = m1.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	_, err = m1.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m1.Produce(context.Background(), "orders", "k", []byte("x")))
	waitFor(t, 2*time.Second, func() bool { return dlqLen(m1, "orders") == 1 })
	testx.RequireNoError(t, m1.Shutdown(context.Background()))
	testx.RequireNoError(t, store1.Close())

	store2, err := NewFileStore(path)
	testx.RequireNoError(t, err)
	defer func() { _ = store2.Close() }()
	m2, err := New(WithStore(store2))
	testx.RequireNoError(t, err)
	defer func() { _ = m2.Shutdown(context.Background()) }()
	_, err = m2.CreateTopic("orders", cfg)
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m2.Recover(context.Background()))
	waitFor(t, 2*time.Second, func() bool { return dlqLen(m2, "orders") == 1 })
	idCh := make(chan string, 1)
	_, err = m2.Subscribe(context.Background(), "orders.dlq", "dg", 1, func(_ context.Context, msg *Message) error {
		idCh <- msg.ID
		return nil
	})
	testx.RequireNoError(t, err)
	gotID := <-idCh
	testx.RequireTrue(t, gotID != "")
	// 死信消费后存储删除：重开恢复应为空。
	time.Sleep(50 * time.Millisecond)
	store3, err := NewFileStore(path)
	testx.RequireNoError(t, err)
	loaded, err := store3.LoadMessages(context.Background())
	testx.RequireNoError(t, err)
	testx.RequireEqual(t, len(loaded), 0)
	testx.RequireNoError(t, store3.Close())
}

// TestDLQStoreSaveFailure 覆盖死信落盘失败仍保留内存副本。
func TestDLQStoreSaveFailure(t *testing.T) {
	fs := &dlqFailStore{}
	m, err := New(WithStore(fs))
	testx.RequireNoError(t, err)
	defer func() { _ = m.Shutdown(context.Background()) }()
	_, err = m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	waitFor(t, 2*time.Second, func() bool { return dlqLen(m, "orders") == 1 })
}

// dlqFailStore 仅在死信落盘时失败。
type dlqFailStore struct {
	fakeStore
}

func (f *dlqFailStore) SaveMessage(_ context.Context, msg *Message) error {
	if strings.HasSuffix(msg.Topic, dlqSuffix) {
		return errors.New("落盘失败")
	}
	return nil
}

// TestDLQStoreDeleteFailure 覆盖死信删除失败仍继续消费。
func TestDLQStoreDeleteFailure(t *testing.T) {
	fs := &fakeStore{deleteErr: errx.New(errx.KindUnavailable, CodeStoreFailed, "删除失败")}
	m, err := New(WithStore(fs))
	testx.RequireNoError(t, err)
	defer func() { _ = m.Shutdown(context.Background()) }()
	_, err = m.CreateTopic("orders", smallTopic())
	testx.RequireNoError(t, err)
	_, err = m.Subscribe(context.Background(), "orders", "g", 1, func(context.Context, *Message) error {
		return errors.New("失败")
	})
	testx.RequireNoError(t, err)
	testx.RequireNoError(t, m.Produce(context.Background(), "orders", "k", nil))
	waitFor(t, 2*time.Second, func() bool { return dlqLen(m, "orders") == 1 })
	var consumed atomic.Int32
	_, err = m.Subscribe(context.Background(), "orders.dlq", "dg", 1, func(context.Context, *Message) error {
		consumed.Add(1)
		return nil
	})
	testx.RequireNoError(t, err)
	waitFor(t, 2*time.Second, func() bool { return consumed.Load() == 1 })
	fs.mu.Lock()
	testx.RequireEqual(t, len(fs.deleted), 2) // 主队列 + 死信各一次
	fs.mu.Unlock()
}

// TestRecoverUnknownDLQ 覆盖恢复时未知死信主题。
func TestRecoverUnknownDLQ(t *testing.T) {
	fs := &fakeStore{loaded: []*Message{{ID: "dlq:a", Topic: "missing.dlq", Key: "k"}}}
	m, err := New(WithStore(fs))
	testx.RequireNoError(t, err)
	defer func() { _ = m.Shutdown(context.Background()) }()
	if err := m.Recover(context.Background()); !errx.Is(err, CodeStoreFailed) {
		t.Fatalf("未知死信主题应报错：%v", err)
	}
}

// TestFileStoreSeamErrors 覆盖文件存储注入失败分支。
func TestFileStoreSeamErrors(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "mq.jsonl")
	fs, err := NewFileStore(path)
	testx.RequireNoError(t, err)
	defer func() { _ = fs.Close() }()

	save := storeSeams()
	storeMarshal = func(any) ([]byte, error) { return nil, errors.New("序列化失败") }
	if err := fs.SaveMessage(ctx, &Message{ID: "a", Topic: "t", Key: "k"}); !errx.Is(err, CodeStoreFailed) {
		t.Fatalf("序列化失败应报错：%v", err)
	}
	save()
	save = storeSeams()
	storeSeek = func(*os.File, int64, int) (int64, error) { return 0, errors.New("定位失败") }
	if err := fs.SaveMessage(ctx, &Message{ID: "a", Topic: "t", Key: "k"}); !errx.Is(err, CodeStoreFailed) {
		t.Fatalf("定位失败应报错：%v", err)
	}
	save()
	save = storeSeams()
	storeWrite = func(*bufio.Writer, []byte) (int, error) { return 0, errors.New("写入失败") }
	if err := fs.SaveMessage(ctx, &Message{ID: "a", Topic: "t", Key: "k"}); !errx.Is(err, CodeStoreFailed) {
		t.Fatalf("写入失败应报错：%v", err)
	}
	save()
	save = storeSeams()
	storeFlush = func(*bufio.Writer) error { return errors.New("刷盘失败") }
	if err := fs.SaveMessage(ctx, &Message{ID: "a", Topic: "t", Key: "k"}); !errx.Is(err, CodeStoreFailed) {
		t.Fatalf("刷盘失败应报错：%v", err)
	}
	save()
	save = storeSeams()
	storeSeek = func(*os.File, int64, int) (int64, error) { return 0, errors.New("定位失败") }
	if _, err := fs.LoadMessages(ctx); !errx.Is(err, CodeStoreFailed) {
		t.Fatalf("定位失败应报错：%v", err)
	}
	save()
}

// TestFileStoreScannerAndCompactSeams 覆盖扫描错误与压实注入分支。
func TestFileStoreScannerAndCompactSeams(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	huge := filepath.Join(dir, "huge.jsonl")
	testx.RequireNoError(t, os.WriteFile(huge,
		[]byte(`{"seq":1,"id":"a"}`+"\n"+string(make([]byte, 17<<20))), 0o600))
	if _, err := NewFileStore(huge); !errx.Is(err, CodeStoreFailed) {
		t.Fatalf("超大行应报错：%v", err)
	}

	blank := filepath.Join(dir, "blank.jsonl")
	testx.RequireNoError(t, os.WriteFile(blank, []byte("\n"), 0o600))
	fs, err := NewFileStore(blank)
	testx.RequireNoError(t, err)
	defer func() { _ = fs.Close() }()
	loaded, err := fs.LoadMessages(ctx)
	testx.RequireNoError(t, err)
	testx.RequireEqual(t, len(loaded), 0)

	// 墓碑先于消息：加载应跳过已删除记录。
	early := filepath.Join(dir, "early.jsonl")
	efs, err := NewFileStore(early)
	testx.RequireNoError(t, err)
	defer func() { _ = efs.Close() }()
	_ = efs.DeleteMessage(ctx, "a")
	_ = efs.SaveMessage(ctx, &Message{ID: "a", Topic: "t", Key: "k"})
	loaded, err = efs.LoadMessages(ctx)
	testx.RequireNoError(t, err)
	testx.RequireEqual(t, len(loaded), 0)
}

// TestCompactSeamErrors 覆盖压实各注入失败分支。
func TestCompactSeamErrors(t *testing.T) {
	ctx := context.Background()
	seams := []struct {
		name    string
		apply   func()
		restore func()
	}{
		{"truncate", func() { storeTruncate = func(*os.File, int64) error { return errors.New("截断失败") } },
			func() { storeSeams() }},
		{"marshal", func() { storeMarshal = func(any) ([]byte, error) { return nil, errors.New("序列化失败") } },
			func() { storeSeams() }},
		{"write", func() { storeWrite = func(*bufio.Writer, []byte) (int, error) { return 0, errors.New("写入失败") } },
			func() { storeSeams() }},
		{"flush", func() { storeFlush = func(*bufio.Writer) error { return errors.New("刷盘失败") } },
			func() { storeSeams() }},
	}
	for _, s := range seams {
		restore := storeSeams()
		path := filepath.Join(t.TempDir(), "mq.jsonl")
		fs, err := NewFileStore(path)
		testx.RequireNoError(t, err)
		testx.RequireNoError(t, fs.SaveMessage(ctx, &Message{ID: "a", Topic: "t", Key: "k"}))
		testx.RequireNoError(t, fs.SaveMessage(ctx, &Message{ID: "b", Topic: "t", Key: "k"}))
		testx.RequireNoError(t, fs.DeleteMessage(ctx, "a"))
		s.apply()
		if _, err := fs.LoadMessages(ctx); !errx.Is(err, CodeStoreFailed) {
			t.Fatalf("%s 失败应报错：%v", s.name, err)
		}
		_ = fs.Close()
		restore()
	}

	// 压实定位失败：扫描时成功、压实重写时失败（第二次 Seek）。
	path := filepath.Join(t.TempDir(), "mq.jsonl")
	fs, err := NewFileStore(path)
	testx.RequireNoError(t, err)
	defer func() { _ = fs.Close() }()
	testx.RequireNoError(t, fs.SaveMessage(ctx, &Message{ID: "a", Topic: "t", Key: "k"}))
	testx.RequireNoError(t, fs.SaveMessage(ctx, &Message{ID: "b", Topic: "t", Key: "k"}))
	testx.RequireNoError(t, fs.DeleteMessage(ctx, "a"))
	calls := 0
	storeSeek = func(f *os.File, off int64, whence int) (int64, error) {
		calls++
		if calls == 2 {
			return 0, errors.New("定位失败")
		}
		return f.Seek(off, whence)
	}
	if _, err := fs.LoadMessages(ctx); !errx.Is(err, CodeStoreFailed) {
		t.Fatalf("压实定位失败应报错：%v", err)
	}
	storeSeams()
}

// storeSeams 返回当前存储接缝快照。
func storeSeams() func() {
	om, ou, os_, ow, of, ot := storeMarshal, storeUnmarshal, storeSeek, storeWrite, storeFlush, storeTruncate
	return func() {
		storeMarshal, storeUnmarshal, storeSeek, storeWrite, storeFlush, storeTruncate = om, ou, os_, ow, of, ot
	}
}
