help:
	@echo "USAGE:"
	@echo "$(MAKE) all      build library + test programs"
	@echo "$(MAKE) lib      build library"
	@echo "$(MAKE) apps     build test programs"
	@echo "$(MAKE) doc      build documentation"
	@echo "$(MAKE) clean    clean everything"

ifeq ($(shell uname -s),FreeBSD)
NPROC=$(shell sysctl kern.smp.cpus|cut -c16- || echo 1)
else
NPROC=$(shell nproc || echo 1)
endif
ifeq ($(strip $(MAKEFLAGS)),)
MAKEFLAGS=-j $(NPROC) --no-print-directory
endif

.PHONY: all lib apps doc gen diff cleandoc clean vclean extra Makefile.local

all: apps

lib:
	@nice -10 $(MAKE) -C src all

apps: lib
	@nice -10 $(MAKE) -C apps all

doc:
	@nice -10 $(MAKE) -C doc $@

gen diff:
	@nice -10 $(MAKE) -C src $@

cleandoc:
	@nice -10 $(MAKE) -C doc clean

clean:
	@nice -10 $(MAKE) -C src $@
	@nice -10 $(MAKE) -C apps $@

vclean: cleandoc clean
	@nice -10 $(MAKE) -C src $@

extra: all
	@nice -10 $(MAKE) -C apps $@

-include Makefile.local # development/testing targets, not for release
