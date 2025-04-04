// derived from code at https://github.com/TechEmpower/FrameworkBenchmarks

package main

import (
	"flag"
	"log"
	"net"
  "runtime"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/reuseport"
)

var (
	clusterSize   = flag.Int("c", 64, "Size of clusters (compatibility only, ignored)")
	scopeCount    = flag.Int("e", 64, "Number of event scopes (compatibility only, ignored)")
	listenerCount = flag.Int("l", 1, "Number of listeners")
	singleServerSocket = flag.Bool("m", true, "socket per listener compatibility only, ignored)")
	threadcount   = flag.Int("t", 1, "Number of system threads")
	listenAddr    = flag.String("addr", ":8800", "TCP address to listen to")
)

func GetListener() net.Listener {
	ln, err := reuseport.Listen("tcp4", *listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	return ln
}

func main() {
	flag.Parse()
  runtime.GOMAXPROCS(*threadcount)
	s := &fasthttp.Server{
		Name:        "go",
		Handler:     mainHandler,
		Concurrency: 1048576,
	}
	for i := 0; i < *listenerCount; i++ {
		ln := GetListener()
		if err := s.Serve(ln); err != nil {
			log.Fatalf("Error when serving incoming connections: %s", err)
		}
	}
}

func mainHandler(ctx *fasthttp.RequestCtx) {
	path := ctx.Path()
	switch string(path) {
	case "/plaintext":
		PlaintextHandler(ctx)
	default:
		ctx.Error("unexpected path", fasthttp.StatusBadRequest)
	}
}

func PlaintextHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/plain")
	ctx.WriteString("Hello, World!")
}
