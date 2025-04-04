include ../Makefile.config
TOPDIR=$(realpath ..)
SOURCES=$(wildcard *.cpp)
TARGETS=$(subst .cpp,,$(SOURCES))
DEPENDS=$(subst .cpp,.d,$(SOURCES))

CFA=cfa
CFAFLAGS=$(CFLAGS)

CFLAGS+=-I. -I../src -I../src/libfibre

LIBS=-lfibre -lrt

ifneq ($(wildcard /usr/include/liburing.h),)
LIBS+=-luring
else
ifneq ($(wildcard /usr/local/include/liburing.h),)
LIBS+=-L/usr/local/lib -Wl,-rpath=/usr/local/lib -luring
endif
endif

ifeq ($(TRACING),1)
LIBS+=-llttng-ust -ldl
endif

ifeq ($(shell uname),FreeBSD)
LIBS+=-lpthread -lexecinfo # needed for backtrace_symbols()
endif

.PHONY: all clean

all: $(TARGETS)

../src/libfibre.so:
	+$(MAKE) -C ../src

# also creates dependencies
$(TARGETS): %: %.cpp ../src/libfibre.so
	$(CXX) $(CXXFLAGS) -MMD $< -o $@ -L../src $(LIBS) -Wl,-rpath=$(TOPDIR)/src

pordering: ordering.cpp
	$(CXX) -DPTHREADS -pthread $< -o $@

gthreadtest: threadtest.go
	go build -o $@ $<

pthreadtest: threadtest.cpp include/pthreads.h
	$(CXX) -Iinclude -DVARIANT=\"pthreads.h\" $(CXXFLAGS) -DPTHREADS $< -o $@ -lrt

pthreadtestcfa: threadtest.cpp include/pthreads.h
	$(CFA) -Iinclude -DVARIANT=\"pthreads.h\" $(CFAFLAGS) -DPTHREADS -x c $< -o $@ -lrt

uthreadtest: threadtest.cpp include/u++.h
	command -v u++ && \
	u++ -Iinclude -DVARIANT=\"u++.h\" $(CXXFLAGS) -multi -nodebug $< -o $@ -lrt || true

AI1=$(shell pwd)/arachne/Arachne/include
AL1=$(shell pwd)/arachne/Arachne/lib
AI2=$(shell pwd)/arachne/CoreArbiter/include
AL2=$(shell pwd)/arachne/CoreArbiter/lib
AI3=$(shell pwd)/arachne/PerfUtils/include
AL3=$(shell pwd)/arachne/PerfUtils/lib
athreadtest: threadtest.cpp include/arachne.h
	[ ! -d $(AL1) ] && true || \
	$(CXX) -Iinclude -DVARIANT=\"arachne.h\" $(CXXFLAGS) -I$(AI1) -I$(AI2) -I$(AI3) $< -o $@ -L$(AL1) -L$(AL2) -L$(AL3) -lArachne -lCoreArbiter -lPerfUtils -lpcrecpp -lrt

BI=/usr/local/include
BL=/usr/local/lib
bthreadtest: threadtest.cpp include/boost.h
	[ ! -d $(BI)/boost ] && true || \
	$(CXX) -Iinclude -DVARIANT=\"boost.h\" $(CXXFLAGS) -I$(BI) $< -o $@ -L$(BL) -lboost_fiber -lboost_context -lrt -Wl,-rpath=$(BL)

QI=$(shell pwd)/qthreads/include
QL=$(shell pwd)/qthreads/src/.libs
qthreadtest: threadtest.cpp include/qthreads.h
	[ ! -d $(QL) ] && true || \
	$(CXX) -Iinclude -DVARIANT=\"qthreads.h\" $(CXXFLAGS) -I$(QI) $< -o $@ -L$(QL) -lqthread -lrt -Wl,-rpath=$(QL)

LI=$(shell pwd)/libfiber/include
LL=$(shell pwd)/libfiber
lthreadtest: threadtest.cpp include/libfiber.h
	[ ! -d $(LL) ] && true || \
	$(CXX) -Iinclude -DVARIANT=\"libfiber.h\" $(CXXFLAGS) -I$(LI) $< -o $@ -L$(LL) -lfiber -lrt -Wl,-rpath=$(LL) -m64 -DARCH_x86_64

MI=$(shell pwd)/mordor
ML=$(shell pwd)/mordor/mordor/.libs
mthreadtest: threadtest.cpp include/mordor.h
	[ ! -d $(ML) ] && true || \
	$(CXX) -Iinclude -DVARIANT=\"mordor.h\" $(CXXFLAGS) -I$(MI) $< -o $@ -L$(ML) -lmordor -lboost_system -lrt -Wl,-rpath=$(ML)

gwebserver: webserver.go
#	go get -u github.com/valyala/fasthttp
#	go get -u github.com/valyala/fasthttp/reuseport
#	go build -gcflags='-l=4' -o $@ $<
	GO111MODULE=auto go build -o $@ $<

pwebserver: webserver.cpp include/pthreads.h
	$(CXX) -Iinclude -DVARIANT=\"pthreads.h\" $(CXXFLAGS) -DPTHREADS $< -o $@ -lrt

uwebserver: webserver.cpp include/u++.h
	command -v u++-web && \
	u++-web -Iinclude -DVARIANT=\"u++.h\" $(CXXFLAGS) -multi -nodebug $< -o $@ -lrt || true

lwebserver: webserver.cpp include/libfiber.h
	[ ! -d $(LL) ] && true || \
	$(CXX) -Iinclude -DVARIANT=\"libfiber.h\" $(CXXFLAGS) -I$(LI) $< -o $@ -L$(LL) -lfiber -lrt -Wl,-rpath=$(LL) -m64 -DARCH_x86_64

fthreadtest: threadtest
	ln -f $< $@

picohttpparser/picohttpparser.h:
	git submodule update --init picohttpparser

webserver: picohttpparser/picohttpparser.h

fwebserver: webserver
	ln -f $< $@

extra: athreadtest bthreadtest fthreadtest gthreadtest pthreadtest \
	fwebserver gwebserver pwebserver pordering

clean:
	rm -f $(TARGETS) $(DEPENDS) pordering
	rm -f athreadtest bthreadtest fthreadtest gthreadtest lthreadtest mthreadtest pthreadtest qthreadtest uthreadtest
	rm -f fwebserver gwebserver lwebserver pwebserver uwebserver

-include Makefile.local # development/testing targets, not for release
-include $(DEPENDS)
