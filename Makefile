compile_protos:
	python -m grpc_tools.protoc -Iprotobuf --python_out=. --grpc_python_out=. protobuf/*.proto

clean:
	rm *_pb2.py
	rm *_pb2_grpc.py
	rm -r ./__pycache__
	rm *.txt
