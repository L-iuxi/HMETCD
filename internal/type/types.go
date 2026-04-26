package types

type LogEntry struct {
	Index    int32
	Term     int32
	Version  int64
	Command  []byte
	ExpireAt int64
}
