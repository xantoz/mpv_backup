ifdef V
Q =
else
Q = @
endif

CC = cc

ROOT = .
BUILD = build

CFLAGS := -I$(ROOT) -I$(BUILD) $(CFLAGS)

OBJECTS = $(SOURCES:.c=.o)

TARGET = mpv

# The /./ -> / is for cosmetic reasons.
BUILD_OBJECTS = $(subst /./,/,$(addprefix $(BUILD)/, $(OBJECTS)))

BUILD_TARGET = $(addprefix $(BUILD)/, $(TARGET))
BUILD_DEPS = $(BUILD_OBJECTS:.o=.d)
CLEAN_FILES += $(BUILD_OBJECTS)

LOG = $(Q) printf "%s\t%s\n"

# Special rules.

all: $(BUILD_TARGET)

clean:
	$(LOG) "CLEAN"
	$(Q) rm -f $(CLEAN_FILES)
	$(Q) rm -rf $(BUILD)/generated/

dist-clean:
	$(LOG) "DIST-CLEAN"
	$(Q) rm -rf $(BUILD)

# Generic pattern rules (used for most source files).

$(BUILD)/%.o: %.c
	$(LOG) "CC" "$@"
	$(Q) mkdir -p $(@D)
	$(Q) $(CC) $(CFLAGS) $< -c -o $@

$(BUILD_TARGET): $(BUILD_OBJECTS)
	$(LOG) "LINK" "$@"
	$(Q) $(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(BUILD_OBJECTS)

.PHONY: all clean .pregen

-include $(BUILD_DEPS)
