#!/usr/bin/env python3

import grpc
import replication_pb2
import replication_pb2_grpc
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc

PRIMARY_PORT = 50051
CLIENT_LOG_FILE = "client.txt"

def input_safe(input_prompt: str, cast_type):
    try: return cast_type(input(input_prompt))
    except ValueError:
        print(f"Error: please input a(n) {cast_type.__name__}!")
        return None

def message_server(port: int):
    key = input("> Key to write? ")
    val = input("> Value to write? ")
    with grpc.insecure_channel(f"localhost:{port}") as channel:
        server_stub = replication_pb2_grpc.SequenceStub(channel)
        res = server_stub.Write(replication_pb2.WriteRequest(key=key, value=val))
        print(f"Server response: {res.ack}")
        with open(CLIENT_LOG_FILE, 'a') as f: f.write(f"{res.ack}\n")

def message_primary():
    message_server(PRIMARY_PORT)

def manual_input():
    server_port = input_safe("> Enter port to write to: ", int)
    message_server(server_port)

def manual_heartbeat():
    server_port = input_safe("> Enter port to heartbeat server: ", int)
    identifier = input("> Enter identifier to use: ")
    with grpc.insecure_channel(f"localhost:{server_port}") as channel:
        server_stub = heartbeat_service_pb2_grpc.ViewServiceStub(channel)
        server_stub.Heartbeat(heartbeat_service_pb2.HeartbeatRequest(service_identifier=identifier))


test_fn = [ message_primary,
            manual_input,
            manual_heartbeat, ]

if __name__ == '__main__':
    with open(CLIENT_LOG_FILE, 'w') as _: print(f"Warning: clearing prior logs in {CLIENT_LOG_FILE}")
    while True:
        print("\nClient operations:")
        for i, func in enumerate(test_fn):
            print(f"[{i}] {func.__name__.replace('_', ' ')}")
        opcode = input_safe(f"Please input opcode (>={len(test_fn)} to quit): ", int)
        while opcode == None: opcode = input_safe("Please input a valid opcode: ", int)
        if opcode >= len(test_fn): break   # Quit the program

        # Execute specified test function
        try: test_fn[opcode]()
        except Exception as e: print(e)
