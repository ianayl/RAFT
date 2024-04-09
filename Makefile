make:
	python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. heartbeat_service.proto
	python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. replication.proto