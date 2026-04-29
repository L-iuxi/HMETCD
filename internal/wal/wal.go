package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrCorrupt  = errors.New("wal: log file is corrupt")
	ErrNotFound = errors.New("wal: file not found")
	ErrClosed   = errors.New("wal: log is closed")
)

type Wal struct {
	file   *os.File
	dir    string
	writer *bufio.Writer
	seq    uint64
}
type LogHeader struct {
	RecType RecType
	Len     int32
	CRC     uint32
}

const (
	recordHeaderSize = 4 + 1 + 4
	//NodeId + LogType + CRC
)

type RecType byte

const (
	RecTypeEntry    RecType = 1 //存储日志条目信息
	RecTypeState    RecType = 2 //存储raft信息
	RecTypeSnapshot RecType = 3 //快照
)

type WalEntry struct {
	Header LogHeader
	Data   []byte
}

func (w *Wal) Dir() string {
	return w.dir
}
func NewWal(path string) *Wal {
	dir := filepath.Dir(path)

	if err := os.Mkdir(dir, 0755); err != nil {
		panic(fmt.Sprintf("failed to create directory: %v", err))
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(fmt.Sprintf("failed to open WAL file: %v", err))
	}

	writer := bufio.NewWriter(file)

	return &Wal{
		file:   file,
		writer: writer,
		dir:    dir,
		seq:    0,
	}
}
func (w *Wal) Write(rectype RecType, data []byte) error {
	if w.writer == nil {
		return ErrClosed
	}

	header := make([]byte, recordHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(1+len(data)+4))
	header[4] = byte(rectype)

	crc := crc32.NewIEEE()
	crc.Write([]byte{byte(rectype)})
	crc.Write(data)
	binary.BigEndian.PutUint32(header[5:9], crc.Sum32())

	if _, err := w.writer.Write(header); err != nil {
		return err
	}

	if _, err := w.writer.Write(data); err != nil {
		return err
	}

	return w.writer.Flush()
}

// LoadAll reads all records from the WAL files.
func (w *Wal) LoadAll() (records [][]byte, types []RecType, err error) {
	// 1. Get list of WAL files
	names, err := listLogFiles(w.dir)
	if err != nil {
		return nil, nil, err
	}

	// 2. Iterate over all WAL files
	for _, name := range names {
		path := filepath.Join(w.dir, name)
		f, err := os.Open(path) // Open each WAL file
		if err != nil {
			return nil, nil, err
		}
		defer f.Close() // Ensure the file is closed when the function exits

		reader := bufio.NewReader(f) // Use buffered reader for better performance
		for {
			// 3. Read header (first 9 bytes)
			header := make([]byte, recordHeaderSize)
			_, err := io.ReadFull(reader, header)
			if err == io.EOF {
				break // End of file, normal exit
			}
			if err != nil {
				return nil, nil, err
			}

			// 4. Decode the header
			length := binary.BigEndian.Uint32(header[0:4])      // Get total length
			recType := RecType(header[4])                       // Get the record type
			expectedCrc := binary.BigEndian.Uint32(header[5:9]) // Get the expected CRC value

			// 5. Read data (length - header size)
			dataLen := length - (1 + 4) // Subtract type (1 byte) and CRC (4 bytes)
			data := make([]byte, dataLen)
			if _, err := io.ReadFull(reader, data); err != nil {
				return nil, nil, err
			}

			// 6. Verify CRC
			crc := crc32.NewIEEE()
			crc.Write([]byte{byte(recType)}) // Write the record type
			crc.Write(data)                  // Write the data
			if crc.Sum32() != expectedCrc {  // Compare CRC values
				return nil, nil, ErrCorrupt // If CRC doesn't match, return error
			}

			// 7. Append record and type to the result slices
			records = append(records, data)
			types = append(types, recType)
		}
	}

	return records, types, nil
}
func listLogFiles(dir string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Filter for .wal files
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".wal") {
			files = append(files, info.Name())
		}
		return nil
	})
	return files, err
}
func (w *Wal) Truncate(lastIndex uint64) error {
	names, err := listLogFiles(w.dir)
	if err != nil {
		return err
	}

	var seq uint64
	for _, name := range names {
		fmt.Sscanf(name, "%x.wal", &seq)
		if seq <= lastIndex {
			if err := os.Remove(filepath.Join(w.dir, name)); err != nil {
				// Log error but continue trying to delete others
			}
		}
	}
	return nil
}
func (w *Wal) Exists() bool {
	_, err := os.Stat(w.dir)
	return err == nil
}
