include ../Makefile.config
SOURCEDIRS=runtime libfibre
SOURCES=$(wildcard $(addsuffix /*.cc,$(SOURCEDIRS)))
GENHEADERS=runtime/testoptions.h runtime-glue/testoptions.h
OBJECTS=$(subst .cc,.o,$(notdir $(SOURCES)))
CSOURCES=$(wildcard $(addsuffix /*.c,$(SOURCEDIRS))) errnoname/errnoname.c
COBJECTS=$(subst .c,.o,$(notdir $(CSOURCES)))
ASOURCES=$(wildcard $(addsuffix /*.S,$(SOURCEDIRS)))
AOBJECTS=$(subst .S,.o,$(notdir $(ASOURCES)))

ifeq ($(TRACING),1)
TSOURCES=$(subst .tp,.c,$(wildcard tracing/*.tp))
TOBJECTS=$(subst .tp,.o,$(wildcard tracing/*.tp))
endif

DEPENDS=$(subst .cc,.d,$(notdir $(SOURCES))) $(subst .c,.d,$(notdir $(CSOURCES))) $(subst .S,.d,$(notdir $(ASOURCES)))
DEPENDS+=$(subst .c,.d,$(notdir $(CSOURCES)))

LIBA=libfibre.a
LIBSO=libfibre.so

CFLAGS+=-I. -D__LIBFIBRE__

vpath %.cc $(SOURCEDIRS)
vpath %.c  $(SOURCEDIRS) errnoname
vpath %.S  $(SOURCEDIRS)

.PHONY: all gen diff clean vclean

all: $(LIBA) $(LIBSO)

gen:
	@rm -f $(GENHEADERS)
	@$(MAKE) $(GENHEADERS)

diff: $(GENHEADERS)
ifeq ($(shell uname -s),FreeBSD)
	@for f in $^; do diff $$f.default $$f; done; true
else
	@for f in $^; do diff -y -W 80 --suppress-common-lines $$f.default $$f; done; true
endif

$(LIBA): $(OBJECTS) $(COBJECTS) $(AOBJECTS) $(TOBJECTS)
	$(AR) cr $@ $^
	ranlib $@

$(LIBSO): $(OBJECTS) $(COBJECTS) $(AOBJECTS) $(TOBJECTS)
	$(CXX) -shared -pthread $^ -o $@

$(GENHEADERS): %: %.default
	@cp $< $@

# also creates dependencies
$(OBJECTS): %.o: %.cc $(GENHEADERS) $(TSOURCES)
	$(CXX) $(CXXFLAGS) -MMD -c $< -o $@

$(COBJECTS): %.o: %.c
	$(CC) $(CFLAGS) -MMD -c $< -o $@

$(AOBJECTS): %.o: %.S
	$(CC) $(CFLAGS) -MMD -c $< -o $@

$(TSOURCES): %.c: %.tp
	lttng-gen-tp $< -o $@ -o $(subst .c,.h,$@)

$(OBJECTS) errnoname/errnoname.c: errnoname/errnoname.h

errnoname/errnoname.h:
	git submodule update --init errnoname

clean:
	rm -f $(LIBA) $(LIBSO) $(OBJECTS) $(COBJECTS) $(AOBJECTS) $(DEPENDS) tracing/*.?

vclean: clean
	rm -f $(GENHEADERS)

-include $(DEPENDS)
