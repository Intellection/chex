.PHONY: all clean

MIX_ENV ?= dev
BUILD_DIR = _build/$(MIX_ENV)
PRIV_DIR = ../../priv

all:
	@mkdir -p $(BUILD_DIR)
	@mkdir -p $(PRIV_DIR)
	@cd $(BUILD_DIR) && cmake ../..
	@cmake --build $(BUILD_DIR) --config $(shell echo $(MIX_ENV) | tr '[:lower:]' '[:upper:]')

clean:
	@rm -rf $(BUILD_DIR)
	@rm -f $(PRIV_DIR)/natch_fine.*
