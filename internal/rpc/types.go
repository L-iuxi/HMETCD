package rpc

type PutArgs struct {
	Key       string
	Value     string
	RequestId int
	ClientId  int
}

type PutReply struct {
	Success bool

	WrongLeader string //后续待添加错误类型
}

type GetArgs struct {
	Key       string
	RequestId int
	ClientId  int
}
type GetReply struct {
	Value   string
	Success bool

	WrongLeader string //后续待添加错误类型
}
