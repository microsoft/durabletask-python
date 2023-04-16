init:
	pip3 install -r requirements.txt

test-unit:
	pytest -m "not e2e" --verbose

test-e2e:
	pytest -m e2e --verbose

install:
	python3 -m pip install .

gen-proto:
# NOTE: There is currently a hand-edit that we make to the generated orchestrator_service_pb2.py file after it's generated to help resolve import problems.
	python3 -m grpc_tools.protoc --proto_path=./submodules/durabletask-protobuf/protos  --python_out=./durabletask/internal --pyi_out=./durabletask/internal --grpc_python_out=./durabletask/internal orchestrator_service.proto

.PHONY: init test-unit test-e2e gen-proto install
