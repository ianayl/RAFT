import grpc
import replication_pb2
import replication_pb2_grpc

def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = replication_pb2_grpc.SequenceStub(channel)

    db = {}
    key, value = None, None
    try:
        while True:
            key = input("Enter key: ")
            value = input("Enter value: ")
            request = replication_pb2.WriteRequest(
                key=key,
                value=value
            )
            try:
                response = stub.Write(request)
                with open('client.txt', 'a') as f:
                    f.write(key + " " + value + "\n")
                db[request.key] = request.value
                print(response.ack)
            except grpc.RpcError as e:
                print(f"Failed to write: {e.code()} - {e.details()}")
    except KeyboardInterrupt:
        print("\nShutting down...")

if __name__ == '__main__':
    run()