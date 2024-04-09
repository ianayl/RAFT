default:
	python3 -m grpc_tools.protoc -Iprotobuf/ --python_out=. --grpc_python_out=. heartbeat_service.proto
	python3 -m grpc_tools.protoc -Iprotobuf/ --python_out=. --grpc_python_out=. raft.proto
	python3 -m grpc_tools.protoc -Iprotobuf/ --python_out=. --grpc_python_out=. replication.proto

clean:
	rm *_pb2.py
	rm *_pb2_grpc.py
	rm -r ./__pycache__
