# SAM custom build: bundle shared `strategies/` package into the Lambda artifact.
# https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-property-function-buildmethod.html

.PHONY: build-TradeExecutorFunction

build-TradeExecutorFunction:
	cp -f *.py "$(ARTIFACTS_DIR)/"
	cp -f requirements.txt "$(ARTIFACTS_DIR)/"
	cp -r ../../strategies "$(ARTIFACTS_DIR)/strategies"
	"$(PYTHON)" -m pip install -r requirements.txt -t "$(ARTIFACTS_DIR)/"
