package clerk

import (
	"context"
	"fmt"
	"math/rand"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"TicketX/proto" // 引入生成的 protobuf 包
)

type Clerk struct {
	Clients   []proto.KvClient
	Leader    int //当前认为的leader
	ClientId  int64
	RequestId int64
}

func NewClerk() *Clerk {
	addrs := []string{
		"localhost:50051",
		"localhost:50052",
		"localhost:50053",
		"localhost:50054",
		"localhost:50055",
	}

	clients := make([]proto.KvClient, len(addrs))

	for i, addr := range addrs {
		conn, err := grpc.Dial(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			panic(err)
		}
		clients[i] = proto.NewKvClient(conn)
	}

	return &Clerk{
		Clients:   clients,
		Leader:    0,
		ClientId:  rand.Int63(),
		RequestId: 1,
	}
}

func TPut(ck *Clerk, key, value string) {
	ck.RequestId++
	req := &proto.PutRequest{
		Key:       key,
		Value:     value,
		RequestId: ck.RequestId,
		ClientId:  ck.ClientId,
	}

	for {
		reply, err := ck.Clients[ck.Leader].Put(context.Background(), req)

		if err != nil {
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			fmt.Println("PUT SUCCESS on leader", ck.Leader)
			return

		case proto.ErrorType_WRONG_LEADER:
			ck.Leader = int(reply.LeaderId)
			fmt.Println("wrong leader → switch", ck.Leader)

		default:
			fmt.Println("internal error or retry")
		}
	}
}
func TGet(ck *Clerk, key string) {
	ck.RequestId++
	req := &proto.GetRequest{
		Key:       key,
		RequestId: ck.RequestId,
		ClientId:  ck.ClientId,
	}

	for {
		reply, err := ck.Clients[ck.Leader].Get(context.Background(), req)
		if err != nil {
			continue
		}

		switch reply.Error {
		case proto.ErrorType_OK:
			fmt.Println("GET SUCCESS,VALUE is", reply.Value)
			return

		case proto.ErrorType_WRONG_LEADER:
			ck.Leader = int(reply.LeaderId)
			fmt.Println("wrong leader → switch", ck.Leader)
		case proto.ErrorType_KEY_NOT_EXIST:
			fmt.Println("THIS KEY IS NOT EXIST")
		default:
			fmt.Println("internal error or retry")
		}
	}
}
func TWatch(ck *Clerk, key string) {
	req := &proto.WatchRequest{
		Key: key,
	}

	for {

		stream, err := ck.Clients[ck.Leader].
			Watch(context.Background(), req)

		if err != nil {

			fmt.Println("watch rpc error:", err)

			continue
		}

		fmt.Println(
			"watch start on leader",
			ck.Leader,
		)

		for {

			resp, err := stream.Recv()

			if err != nil {

				fmt.Println(
					"watch stream closed:",
					err,
				)

				break
			}

			switch resp.Err {

			case proto.ErrorType_OK:

				fmt.Printf(
					"[WATCH] type=%s key=%s value=%s revision=%d\n",
					resp.Type,
					resp.Key,
					resp.Value,
					resp.Revision,
				)

			case proto.ErrorType_WRONG_LEADER:

				ck.Leader = int(resp.LeaderId)

				fmt.Println(
					"wrong leader -> switch to",
					ck.Leader,
				)

				break

			default:

				fmt.Println(
					"watch internal error",
				)
			}
		}
	}
}
