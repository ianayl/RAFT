import grpc
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc
import time
import threading

from concurrent import futures

shutdown = False

statusMap = {
    "primary": [15, None],
    "backup": [15, None]
}

class ViewServiceServicer(heartbeat_service_pb2_grpc.ViewServiceServicer):
    def Heartbeat(self, request, context):
        statusMap[request.service_identifier][0] = 15
        statusMap[request.service_identifier][1] = time.strftime("%a, %d %b %Y %H:%M:%S %z", time.localtime())
        with open('heartbeat.txt', 'a') as f:
            f.write(f"{request.service_identifier.capitalize()} is alive. Latest heartbeat recieved at {statusMap[request.service_identifier][1]}\n")
        return heartbeat_service_pb2.google_dot_protobuf_dot_empty__pb2.Empty()

def logger():
    while not shutdown:
        for key in statusMap:
            if statusMap[key][0] == 0:
                with open('heartbeat.txt', 'a') as f:
                    f.write(f"{key.capitalize()} might be down. Latest heartbeat recieved at {statusMap[key][1]}\n")
            statusMap[key][0] -= 1
        time.sleep(1)

def serve():
    try:
        print("Heartbeat service started at port 50053")
        loggerThread = threading.Thread(target=logger)
        loggerThread.start()
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        heartbeat_service_pb2_grpc.add_ViewServiceServicer_to_server(ViewServiceServicer(), server)
        server.add_insecure_port('[::]:50053')
        server.start()
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        global shutdown
        shutdown = True
        loggerThread.join()
        server.stop(0)
        
if __name__ == '__main__':
    serve()