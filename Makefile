# Qallow C targets. Rust workspace builds via `cargo build`; this file
# covers the standalone C modules and their tests.
CC      ?= cc
CFLAGS  ?= -std=c11 -Wall -Wextra -Werror -O2
# MDB_MAXKEYSIZE matches QSW_MAX_KEY_LEN (sync_wire.h); LMDB's default
# (511) is smaller and would reject valid envelopes.
CPPFLAGS += -Iinclude -Ithird_party/lmdb -DMDB_MAXKEYSIZE=1024
BUILDDIR := build

TESTS := $(BUILDDIR)/test_sync_wire $(BUILDDIR)/test_persist_lmdb
LMDB_CFLAGS := -Wno-error -Wno-unused-parameter
BUILDDIR_AARCH64 := $(BUILDDIR)/aarch64
AARCH64_CC ?= aarch64-linux-gnu-gcc

.PHONY: test clean

test: $(TESTS)
	@set -e; for t in $(TESTS); do ./$$t; done

$(BUILDDIR)/test_sync_wire: src/mind/sync_wire.c tests/test_sync_wire.c include/qallow/sync_wire.h | $(BUILDDIR)
	$(CC) $(CFLAGS) $(CPPFLAGS) src/mind/sync_wire.c tests/test_sync_wire.c -o $@

$(BUILDDIR)/test_persist_lmdb: src/mind/persist_lmdb.c src/mind/sync_wire.c \
		tests/test_persist_lmdb.c \
		include/qallow/persist_lmdb.h include/qallow/sync_wire.h \
		third_party/lmdb/mdb.c third_party/lmdb/midl.c | $(BUILDDIR)
	$(CC) $(CFLAGS) $(LMDB_CFLAGS) $(CPPFLAGS) src/mind/persist_lmdb.c src/mind/sync_wire.c \
		tests/test_persist_lmdb.c \
		third_party/lmdb/mdb.c third_party/lmdb/midl.c -o $@

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

clean:
	rm -rf $(BUILDDIR)

.PHONY: test-aarch64
test-aarch64: $(BUILDDIR_AARCH64)/test_sync_wire $(BUILDDIR_AARCH64)/test_persist_lmdb
	@echo "aarch64 cross-compile completed"

$(BUILDDIR_AARCH64):
	mkdir -p $(BUILDDIR_AARCH64)

$(BUILDDIR_AARCH64)/test_sync_wire: src/mind/sync_wire.c tests/test_sync_wire.c include/qallow/sync_wire.h | $(BUILDDIR_AARCH64)
	$(AARCH64_CC) $(CFLAGS) $(CPPFLAGS) -c src/mind/sync_wire.c -o $(BUILDDIR_AARCH64)/sync_wire.o
	$(AARCH64_CC) $(CFLAGS) $(CPPFLAGS) -c tests/test_sync_wire.c -o $(BUILDDIR_AARCH64)/test_sync_wire.o
	$(AARCH64_CC) $(CFLAGS) $(CPPFLAGS) $(BUILDDIR_AARCH64)/sync_wire.o $(BUILDDIR_AARCH64)/test_sync_wire.o -o $@

$(BUILDDIR_AARCH64)/test_persist_lmdb: src/mind/persist_lmdb.c src/mind/sync_wire.c \
		tests/test_persist_lmdb.c \
		include/qallow/persist_lmdb.h include/qallow/sync_wire.h \
		third_party/lmdb/mdb.c third_party/lmdb/midl.c | $(BUILDDIR_AARCH64)
	$(AARCH64_CC) $(CFLAGS) $(LMDB_CFLAGS) $(CPPFLAGS) -c src/mind/persist_lmdb.c -o $(BUILDDIR_AARCH64)/persist_lmdb.o
	$(AARCH64_CC) $(CFLAGS) $(LMDB_CFLAGS) $(CPPFLAGS) -c src/mind/sync_wire.c -o $(BUILDDIR_AARCH64)/sync_wire_persist.o
	$(AARCH64_CC) $(CFLAGS) $(LMDB_CFLAGS) $(CPPFLAGS) -c tests/test_persist_lmdb.c -o $(BUILDDIR_AARCH64)/test_persist_lmdb.o
	$(AARCH64_CC) $(CFLAGS) $(LMDB_CFLAGS) $(CPPFLAGS) -c third_party/lmdb/mdb.c -o $(BUILDDIR_AARCH64)/mdb.o
	$(AARCH64_CC) $(CFLAGS) $(LMDB_CFLAGS) $(CPPFLAGS) -c third_party/lmdb/midl.c -o $(BUILDDIR_AARCH64)/midl.o
	$(AARCH64_CC) $(CFLAGS) $(LMDB_CFLAGS) $(CPPFLAGS) $(BUILDDIR_AARCH64)/persist_lmdb.o \
		$(BUILDDIR_AARCH64)/sync_wire_persist.o $(BUILDDIR_AARCH64)/test_persist_lmdb.o \
		$(BUILDDIR_AARCH64)/mdb.o $(BUILDDIR_AARCH64)/midl.o -o $@
