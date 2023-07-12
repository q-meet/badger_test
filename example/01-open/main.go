package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	badger "github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/ristretto/z"
	"log"
	"time"
)

var commonKey = []byte("answer")

func main() {
	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.

	// 写入内存
	// opt := badger.DefaultOptions("").WithInMemory(true)

	//db. = 100 << 20 // 100 mb or some other size based on the amount of data
	db, err := badger.Open(badger.DefaultOptions("./tmp/badger").WithIndexCacheSize(100 << 20))
	if err != nil {
		log.Fatal(err)
	}

	defer func(db *badger.DB) {
		err = db.Close()
		if err != nil {
			panic(err)
		}
	}(db)

	// 要启动只读事务，可以使用以下DB.View()方法：
	err = db.View(func(txn *badger.Txn) error {
		get, err := txn.Get([]byte("answer111111111111"))
		if err != nil {
			return err
		}

		val, err := get.ValueCopy(nil)
		fmt.Printf("val %s, err:%v \n", val, err)
		// Your code here…
		//Txn.Get()ErrKeyNotFound如果未找到该值则返回。

		//请注意，返回的值Get()仅在交易打开时有效。如果您需要在事务之外使用值，则必须使用copy()将其复制到另一个字节片。

		//使用该Txn.Delete()方法删除一个键。

		return nil
	})

	fmt.Printf("err %v \n", err)

	// 要启动读写事务，可以使用以下DB.Update()方法
	err = db.Update(func(txn *badger.Txn) error {
		err = txn.Set(commonKey, []byte("42"))
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
	// 手动管理事物
	err = Read(db)
	if err != nil {
		log.Fatal(err)
	}
	// 单调递增整数
	err = Incr(db)
	if err != nil {
		log.Fatal(err)
	}
	// 单调递增整数
	err = merge(db)
	if err != nil {
		log.Fatal(err)
	}

}

// Incr 单调递增整数
func Incr(db *badger.DB) (err error) {
	seq, err := db.GetSequence([]byte("number1"), 1000)
	defer seq.Release()

	num, err := seq.Next()
	num, err = seq.Next()
	num, err = seq.Next()
	num, err = seq.Next()

	fmt.Printf("value:%d, err:%v", num, err)

	value, err := readKey(db, []byte("number1"))
	fmt.Printf("\n\n\n\n value:%s, err:%v", value, err)
	return nil
}

// Merge function to append one byte slice to another
func add(originalValue, newValue []byte) []byte {
	return append(originalValue, newValue...)
}

// 增加计数器的合并运算符
func uint64ToBytes(i uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], i)
	return buf[:]
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Merge function to add two uint64 numbers
func addInt(existing, new []byte) []byte {
	return uint64ToBytes(bytesToUint64(existing) + bytesToUint64(new))
}

func merge(db *badger.DB) error {
	/*
		DB.GetMergeOperator()然后可以将该函数与键和持续时间值一起传递给该方法。持续时间指定对使用该方法添加的值运行合并函数的频率MergeOperator.Add() 。
		MergeOperator.Get()方法可用于检索与合并操作关联的键的累积值。
	*/
	key := []byte("merge")

	m := db.GetMergeOperator(key, add, 200*time.Millisecond)
	defer m.Stop()

	m.Add([]byte("A"))
	m.Add([]byte("B"))
	m.Add([]byte("C"))

	res, _ := m.Get() // res should have value ABC encoded
	fmt.Printf("res:%s\n\n", res)

	key = []byte("merge_int")

	m1 := db.GetMergeOperator(key, addInt, 200*time.Millisecond)
	defer m1.Stop()

	m1.Add(uint64ToBytes(1))
	m1.Add(uint64ToBytes(2))
	m1.Add(uint64ToBytes(3))

	res, _ = m1.Get() // res should have value 6 encoded
	fmt.Printf("res:%s\n\n", res)

	handle(Iterator(db))
	handle(PrefixScan(db))
	return nil
}

// 设置密钥的生存时间 (TTL) 和用户元数据
func setTTL(db *badger.DB) error {
	err := db.Update(func(txn *badger.Txn) error {
		// Badger 允许在键上设置可选的生存时间 (TTL) 值。一旦 TTL 过去，密钥将不再可检索，并且有资格进行垃圾回收。time.Duration可以使用Entry.WithTTL() 和API 方法将 TTL 设置为值Txn.SetEntry()。

		// 可以在每个键上设置可选的用户元数据值。用户元数据值由单个字节表示。它可用于设置某些位以及密钥，以帮助解释或解码键值对。Entry.WithMeta()可以使用API方法设置用户元数据Txn.SetEntry()。
		e := badger.NewEntry([]byte("answer"), []byte("42")).WithMeta(byte(1)).WithTTL(time.Hour)
		err := txn.SetEntry(e)
		return err
	})
	return err
}

// Iterator 迭代器
func Iterator(db *badger.DB) error {
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				fmt.Printf("key=%s, value=%s\n", k, v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// IteratorKey 迭代器 仅键迭代
func IteratorKey(db *badger.DB) error {
	// Badger 支持一种独特的迭代模式，称为“仅键迭代”。它比常规迭代快几个数量级，因为它只涉及对 LSM 树的访问，而 LSM 树通常完全驻留在 RAM 中。要启用仅键迭代，您需要将该IteratorOptions.PrefetchValues 字段设置为false。这也可以用于在迭代期间通过item.Value()仅在需要时调用来对选定键进行稀疏读取。
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			fmt.Printf("key=%s\n", k)
		}
		return nil
	})

	return err
}

/**
 * PrefixScan
 * 前缀扫描
 */
func PrefixScan(db *badger.DB) error {
	// 要迭代键前缀，您可以组合Seek()和ValidForPrefix()
	err := db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("m")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				fmt.Printf("Prefix key=%s, value=%s\n", k, v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

// 流
func Stream(db *badger.DB) error {
	stream := db.NewStream()
	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = 16                     // Set number of goroutines to use for iteration.
	stream.Prefix = []byte("some-prefix") // Leave nil for iteration over the whole DB.
	stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.

	// ChooseKey is called concurrently for every key. If left nil, assumes true by default.
	stream.ChooseKey = func(item *badger.Item) bool {
		return bytes.HasSuffix(item.Key(), []byte("er"))
	}

	// KeyToList is called concurrently for chosen keys. This can be used to convert
	// Badger data into custom key-values. If nil, uses stream.ToList, a default
	// implementation, which picks all valid key-values.
	stream.KeyToList = nil

	// -- End of optional settings.

	// Send is called serially, while Stream.Orchestrate is running.
	stream.Send = func(buf *z.Buffer) error {
		return nil
	}

	// Run the stream
	if err := stream.Orchestrate(context.Background()); err != nil {
		return err
	}
	// Done.

	return nil
}

// GarbageCollection 垃圾收集
func GarbageCollection(db *badger.DB) error {
	/*
		Badger 值需要进行垃圾收集，原因有两个：

		Badger 将值与 LSM 树分开保存。这意味着清理 LSM 树的压缩操作根本不会触及这些值。值需要单独清理。

		并发读/写事务可能会为单个键留下多个值，因为它们存储为不同的版本。这些可能会累积，并在需要这些旧版本之后占用不必要的空间。

		Badger 依赖客户端在他们选择的时间执行垃圾收集。它提供了以下方法，可以在适当的时候调用：

		DB.RunValueLogGC()：此方法旨在在 Badger 在线时进行垃圾收集。除了随机挑选文件之外，它还使用 LSM 树压缩生成的统计信息来挑选可能导致最大空间回收的文件。建议在系统活动较少期间或定期调用。一次调用只会导致最多删除一个日志文件。作为一种优化，您还可以在返回 nil 错误（表示成功的值日志 GC）时立即重新运行它，如下所示。
	*/
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
	again:
		err := db.RunValueLogGC(0.7)
		if err == nil {
			goto again
		}
	}
	return nil
}

// 内存使用情况
func MemoryUsage(db *badger.DB) error {
	/*OptionsBadger 的内存使用情况可以通过调整使用打开数据库时传入的结构 中的几个可用选项来管理DB.Open。

	内存表数量 ( Options.NumMemtables)
	如果修改Options.NumMemtables，也要进行相应Options.NumLevelZeroTables调整 Options.NumLevelZeroTablesStall。
	并发压缩数 ( Options.NumCompactors)
	桌子尺寸 ( Options.MaxTableSize)
	值日志文件的大小 ( Options.ValueLogFileSize)
	如果您想减少 Badger 实例的内存使用量，请调整这些选项（最好一次一个），直到达到所需的内存使用量。*/
	return nil
}

// 数据库备份
func Backup() {
	/*有两个公共API方法DB.Backup()可DB.Load()用于进行在线备份和恢复。Badger v0.9提供了一个CLI工具 badger，可以进行离线备份/恢复。确保您$GOPATH/bin 的路径中有使用此工具的路径。

	  下面的命令将创建与版本无关的数据库备份到badger.bak当前工作目录中的文件

	  badger backup --dir <path/to/badgerdb>
	  复制
	  要将badger.bak当前工作目录恢复到新数据库：

	  badger restore --dir <path/to/badgerdb>
	  复制
	  请参阅badger --help了解更多详情。

	  如果您有使用 v0.8（或更低版本）创建的 Badger 数据库，则可以使用badger_backupv0.8.1 中提供的工具，然后使用上面的命令恢复它以升级数据库以使用最新版本。

	  badger_backup --dir <path/to/badgerdb> --backup-file badger.bak
	  复制
	  我们建议所有用户使用BackupAPIRestore和工具。然而，Badger 也是 rsync 友好的，因为所有文件都是不可变的，除了仅追加的最新值日志。因此，rsync 可以用作执行备份的基本方法。在以下脚本中，我们重复 rsync 以确保在进行完整备份时 LSM 树与 MANIFEST 文件保持一致。

	  #!/bin/bash
	  set -o history
	  set -o histexpand
	  # Makes a complete copy of a Badger database directory.
	  # Repeat rsync if the MANIFEST and SSTables are updated.
	  rsync -avz --delete db/ dst
	  while !! | grep -q "(MANIFEST\|\.sst)$"; do :; done
	*/

}

/**
 * 读取密钥
 */
func readKey(db *badger.DB, key []byte) (value []byte, err error) {
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}

	return
}

func Read(db *badger.DB) error {
	// Start a writable transaction.
	// 手动管理事物
	txn := db.NewTransaction(true)
	defer txn.Discard()

	item, err := txn.Get(commonKey)
	println("item.String()", item.String())

	err = item.Value(func(val []byte) error {
		// This func with val would only be called if item.Value encounters no error.

		// Accessing val here is valid.
		fmt.Printf("The answer is: %s\n", val)

		// Copying or parsing val is valid.
		return nil
	})

	// Use the transaction...
	err = txn.Set(commonKey, []byte("423"))
	if err != nil {
		return err
	}

	println("txn.set 423")

	item, err = txn.Get(commonKey)
	println("item.String()", item.String())

	var valNot, valCopy []byte
	err = item.Value(func(val []byte) error {
		// This func with val would only be called if item.Value encounters no error.

		// Accessing val here is valid.
		fmt.Printf("The answer is: %s\n", val)

		// Copying or parsing val is valid.
		valCopy = append([]byte{}, val...)

		// Assigning val slice to another variable is NOT OK.
		valNot = val // Do not do this.
		return nil
	})
	handle(err)

	// DO NOT access val here. It is the most common cause of bugs.
	fmt.Printf("NEVER do this. %s\n", valNot)

	// You must copy it to use it outside item.Value(...).
	fmt.Printf("The answer is: %s\n", valCopy)

	// Alternatively, you could also use item.ValueCopy().
	valCopy, err = item.ValueCopy(nil)
	handle(err)
	fmt.Printf("The answer is: %s\n", valCopy)

	// Commit the transaction and check for error.
	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

func handle(err error) {
	if err != nil {
		panic(err)
	}
}
