import grpc
import replication_pb2
import replication_pb2_grpc
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc
import time
import threading

from concurrent import futures

shutdown = False
db = {}

class SequenceServicer(replication_pb2_grpc.SequenceServicer):

    def Write(self, request, context):
        try:
            # forward request to backup
            channel = grpc.insecure_channel('localhost:50052')
            stub = replication_pb2_grpc.SequenceStub(channel)
            response = stub.Write(request) 
            print(response.ack)

            # write to server.txt
            with open('server.txt', 'a') as f:
                f.write(request.key + " " + request.value + "\n")

            # write to db
            db[request.key] = request.value

        except grpc.RpcError as e: # if backup is down
            print(f"Failed to write: {e.code()} - {e.details()}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details('Backup is inaccessible.')

        return replication_pb2.WriteResponse(ack="Received key: " + request.key + " and value: " + request.value)

def heartbeat():
    channel = grpc.insecure_channel('localhost:50053')
    stub = heartbeat_service_pb2_grpc.ViewServiceStub(channel)
    stub.Heartbeat(heartbeat_service_pb2.HeartbeatRequest(service_identifier="primary"))

def heartbeatService():
    while not shutdown:
        try:
            heartbeat()
        except grpc.RpcError as e:
            print(f"{e.code()}: {e.details()}")
            print("Unable to access heartbeat server. Retrying...")
        finally:
            time.sleep(5)

def serve():
    try:
        print("Server started at port 50051")
        heartbeatThread = threading.Thread(target=heartbeatService)
        heartbeatThread.start()
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        replication_pb2_grpc.add_SequenceServicer_to_server(SequenceServicer(), server)
        server.add_insecure_port('[::]:50051')
        server.start()
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        global shutdown
        shutdown = True
        heartbeatThread.join()
        server.stop(0)

if __name__ == '__main__':
    serve()