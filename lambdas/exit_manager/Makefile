# SAM custom build: bundle shared `strategies/` into the Lambda artifact.

.PHONY: build-ExitManagerFunction

build-ExitManagerFunction:
	cp -f *.py "$(ARTIFACTS_DIR)/"
	cp -f requirements.txt "$(ARTIFACTS_DIR)/"
	cp -r ../../strategies "$(ARTIFACTS_DIR)/strategies"
	"$(PYTHON)" -m pip install -r requirements.txt -t "$(ARTIFACTS_DIR)/"
