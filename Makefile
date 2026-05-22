init:
	pip3 install -r requirements.txt

test:
	pytest --verbose

install:
	python3 -m pip install .

gen-proto:
	curl -o durabletask/internal/orchestrator_service.proto https://raw.githubusercontent.com/microsoft/durabletask-protobuf/refs/heads/main/protos/orchestrator_service.proto
	curl -H "Accept: application/vnd.github.v3+json" "https://api.github.com/repos/microsoft/durabletask-protobuf/commits?path=protos/orchestrator_service.proto&sha=main&per_page=1" | jq -r '.[0].sha' >> durabletask/internal/PROTO_SOURCE_COMMIT_HASH
	python3 -m grpc_tools.protoc --proto_path=.  --python_out=. --pyi_out=. --grpc_python_out=. ./durabletask/internal/orchestrator_service.proto
	rm durabletask/internal/*.proto

SERVERLESS_PROTO_REPO ?= ../durabletask-protobuf

gen-serverless-proto:
	cp $(SERVERLESS_PROTO_REPO)/protos/serverless_activities_service.proto durabletask-azuremanaged/durabletask/azuremanaged/internal/serverless_activities_service.proto
	git -C $(SERVERLESS_PROTO_REPO) log -n 1 --format=%H -- protos/serverless_activities_service.proto > durabletask-azuremanaged/durabletask/azuremanaged/internal/SERVERLESS_PROTO_SOURCE_COMMIT_HASH
	python3 -m grpc_tools.protoc --proto_path=durabletask-azuremanaged --python_out=durabletask-azuremanaged --pyi_out=durabletask-azuremanaged --grpc_python_out=durabletask-azuremanaged durabletask-azuremanaged/durabletask/azuremanaged/internal/serverless_activities_service.proto

.PHONY: init test gen-proto gen-serverless-proto install
