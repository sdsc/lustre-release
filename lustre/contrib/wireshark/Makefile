MIN_WIRESHARK_VERSION=1.6.8
# Modify to point to your Wireshark and glib include directories
#INCS = -I$(HOME)/wireshark-1.6.8 `pkg-config --libs --cflags glib-2.0`
INCS=$(shell pkg-config --libs --cflags wireshark)
# If not using above pkg-config, comment out this line
CHECK=pkg-config --atleast-version=${MIN_WIRESHARK_VERSION} wireshark
# If not using installed wireshark, change this version
WS_VERSION=$(shell pkg-config --modversion wireshark)

SRCS_LNET = packet-lnet.c
SRCS_LUSTRE = packet-lustre.c

CC   = gcc

OBJS_LNET = $(foreach src, $(SRCS_LNET), $(src:.c=.o))
OBJS_LUSTRE = $(foreach src, $(SRCS_LUSTRE), $(src:.c=.o))

PLUGINS=lnet.so lustre.so

ifeq ($(shell id -u), 0)
PLUGIN_DIR = /usr/local/lib/wireshark/plugins/$(WS_VERSION)
else
PLUGIN_DIR = ${HOME}/.wireshark/plugins
endif

ifeq (${CHECK},)
	CHECK=true
endif

CFLAGS = -DHAVE_CONFIG_H $(INCS) -DINET6 -D_U_=__attribute__\(\(unused\)\) -Wall -Wpointer-arith -g -DXTHREADS -D_REENTRANT -DXUSE_MTSAFE_API -fPIC -DPIC

all: $(PLUGINS)

.PHONEY: check
check:
	@if ! ${CHECK}; then\
		echo "Wireshark must be at least version ${MIN_WIRESHARK_VERSION} (installed ${WS_VERSION})";\
	false; fi

lustre.so: check $(OBJS_LUSTRE)
	$(CC) -shared $(OBJS_LUSTRE) -o $@

lnet.so: check $(OBJS_LNET)
	$(CC) -shared $(OBJS_LNET) -o $@

install: $(PLUGINS)
	mkdir -p $(PLUGIN_DIR)
	install $(PLUGINS) $(PLUGIN_DIR)/

clean:
	rm -f $(PLUGINS) $(OBJS_LNET) $(OBJS_LUSTRE)

extraclean: clean
	(cd $(PLUGIN_DIR)/; rm -f $(PLUGINS))
