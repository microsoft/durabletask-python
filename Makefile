init:
	pip3 install -r requirements.txt

test-unit:
	pytest -m "not e2e" --verbose

test-e2e:
	pytest -m e2e --verbose

install:
	python3 -m pip install .

gen-proto:
	curl -o durabletask/internal/orchestrator_service.proto https://raw.githubusercontent.com/microsoft/durabletask-protobuf/refs/heads/main/protos/orchestrator_service.proto
	python3 -m grpc_tools.protoc --proto_path=.  --python_out=. --pyi_out=. --grpc_python_out=. ./durabletask/internal/orchestrator_service.proto
	rm durabletask/internal/*.proto

.PHONY: init test-unit test-e2e gen-proto install
