init:
	pip3 install -r requirements.txt

test:
	pytest --verbose

install:
	python3 -m pip install .

gen-proto:
	curl -o durabletask/internal/orchestrator_service.proto https://raw.githubusercontent.com/microsoft/durabletask-protobuf/refs/heads/main/protos/orchestrator_service.proto
	curl -H "Accept: application/vnd.github.v3+json" "https://api.github.com/repos/microsoft/durabletask-protobuf/commits?path=protos/orchestrator_service.proto&sha=main&per_page=1" | jq -r '.[0].sha' > durabletask/internal/PROTO_SOURCE_COMMIT_HASH
	python3 -m grpc_tools.protoc --proto_path=.  --python_out=. --pyi_out=. --grpc_python_out=. ./durabletask/internal/orchestrator_service.proto
	rm durabletask/internal/*.proto
	ON_DEMAND_SANDBOX_PROTO_SOURCE_COMMIT_HASH=$$(cat durabletask-azuremanaged/durabletask/azuremanaged/internal/ON_DEMAND_SANDBOX_PROTO_SOURCE_COMMIT_HASH); \
	curl -o durabletask-azuremanaged/durabletask/azuremanaged/internal/on_demand_sandbox_activities_service.proto https://raw.githubusercontent.com/microsoft/durabletask-protobuf/$${ON_DEMAND_SANDBOX_PROTO_SOURCE_COMMIT_HASH}/protos/serverless_activities_service.proto
	python3 -m grpc_tools.protoc --proto_path=. --python_out=. --pyi_out=. --grpc_python_out=. ./durabletask-azuremanaged/durabletask/azuremanaged/internal/on_demand_sandbox_activities_service.proto
	rm durabletask-azuremanaged/durabletask/azuremanaged/internal/*.proto

.PHONY: init test gen-proto install
