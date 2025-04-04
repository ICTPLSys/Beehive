/******************************************************************************
    Copyright (C) Martin Karsten 2015-2023

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
#include "fibre.h"

#include <iostream>
#include <unistd.h> // getopt, close
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

#if defined(__FreeBSD__)
#include <netinet/in.h>
#endif

/*-----------------------------------------------------------------------------
 * NOTE: For stress-testing, the backlog parameter in 'listen' is set to the
 * number of connections in 'servmain' below.  Also:
 *
 * sysctl net.ipv4.tcp_max_syn_backlog
 *
 * and
 *
 * /proc/sys/net/core/somaxconn
 *
 * need to be set to similarly high numbers.  Otherwise, the kernel will
 * suspect a SYN flooding attack and eventually respond with TCP resets
 * (RST) to new connection requests.  This in turn makes this test program
 * fail, because a socket I/O call fails with ECONNRESET.  See
 *
 * https://serverfault.com/questions/294209/possible-syn-flooding-in-log-despite-low-number-of-syn-recv-connections
 *
 * for a good description.
-----------------------------------------------------------------------------*/

static const int numaccept = 2;

static bool server = false;
static int numconn = std::max(2,numaccept);
static int clntcount = 0;
static int acceptcount = 0;
static int pausecount = -1;

static void usage(const char* prog) {
  std::cerr << "usage: " << prog << " -a <addr> -c <conns> -p <port> [-s] -t <pause count>" << std::endl;
}

static void opts(int argc, char** argv, sockaddr_in& addr) {
  struct addrinfo  hint = { 0, AF_INET, SOCK_STREAM, 0, 0, nullptr, nullptr, nullptr };
  struct addrinfo* info = nullptr;
  for (;;) {
    int option = getopt( argc, argv, "a:c:hp:st:?" );
    if ( option < 0 ) break;
    switch(option) {
    case 'a':
      SYSCALL(getaddrinfo(optarg, nullptr, &hint, &info));
      addr.sin_addr = ((sockaddr_in*)info->ai_addr)->sin_addr; // already filtered for AF_INET
      freeaddrinfo(info);
      break;
    case 'c':
      numconn = atoi(optarg);
      break;
    case 'p':
      addr.sin_port = htons(atoi(optarg));
      break;
    case 's':
      server = true;
      break;
    case 't':
      pausecount = atoi(optarg);
      break;
    case 'h':
    case '?':
      usage(argv[0]);
      exit(1);
    default:
      std::cerr << "unknown option -" << (char)option << std::endl;
      usage(argv[0]);
      exit(1);
    }
  }
  if (argc != optind) {
    std::cerr << "unknown argument - " << argv[optind] << std::endl;
    usage(argv[0]);
    exit(1);
  }
}

static void servconn(void* arg) {
  // report client connection
  intptr_t fd = (intptr_t)arg;
  struct sockaddr_in client;
  socklen_t addrlen = sizeof(client);
  SYSCALL(getpeername(fd, (sockaddr*)&client, &addrlen));
  std::cout << fd << " connection from " << inet_ntoa(client.sin_addr) << ':' << ntohs(client.sin_port) << std::endl;

  // read input string and compute return as XOR of input
  char code = 0;
  for (;;) {
    char buf[128];
    int len = SYSCALL_GE(lfRecv(fd, (void*)buf, sizeof(buf), 0), 1);
    for (int i = 0; i < len; i += 1) {
      if (buf[i] == 127) goto finish;
      code = code ^ buf[i];
    }
  }

finish:
//  std::cout << ntohs(client.sin_port) << " done receiving" << std::endl;
  // send response
  SYSCALL_EQ(lfSend(fd, (const void*)&code, sizeof(code), 0), sizeof(code));
//  std::cout << ntohs(client.sin_port) << " done sending" << std::endl;

  // wait for client closing the socket
  SYSCALL_EQ(lfRecv(fd, (void*)&code, sizeof(code), 0), 0);
//  std::cout << ntohs(client.sin_port) << " client closed" << std::endl;

  // close socket and terminate fibre
  SYSCALL(lfClose(fd));
}

static int servFD = -1;

void servaccept() {
  // create up to N connection handlers
  Fibre** f = new Fibre*[numconn/numaccept];
  for (int n = 0; n < numconn/numaccept; n += 1) {
    intptr_t fd = SYSCALLIO(lfAccept(servFD, nullptr, nullptr));
    __atomic_add_fetch(&acceptcount, 1, __ATOMIC_RELAXED);
    if (acceptcount == pausecount) {
      Context::CurrCluster().pause();
      std::cout << "world stopped" << std::endl;
#if 0
      char c;
      std::cin >> c;
#else
      sleep(2);
#endif
      std::cout << "world resumes" << std::endl;
      Context::CurrCluster().resume();
    }
    f[n] = (new Fibre)->run(servconn, (void*)fd);
  }

  // wait until all handlers are finished
  for (int n = 0; n < numconn/numaccept; n += 1) {
    delete f[n];
    std::cout << "joined " << n << std::endl;
  }
  delete [] f;
}

void servaccept2(void*) {
  Context::CurrCluster().addWorkers(2);
  servaccept();
  std::cout << "finishing 2nd accept loop" << std::endl;
}

void servmain(sockaddr_in& addr) {
  // create server socket
  servFD = SYSCALLIO(lfSocket(AF_INET, SOCK_STREAM, 0));
  int on = 1;
  SYSCALL(setsockopt(servFD, SOL_SOCKET, SO_REUSEADDR, (const void*)&on, sizeof(int)));

  // bind to server address
  SYSCALL(lfBind(servFD, (sockaddr*)&addr, sizeof(addr)));

  // query and report addressing info
  socklen_t addrlen = sizeof(addr);
  SYSCALL(getsockname(servFD, (sockaddr*)&addr, &addrlen));
  std::cout << "listening on " << inet_ntoa(addr.sin_addr) << ':' << ntohs(addr.sin_port) << std::endl;
  SYSCALL(lfListen(servFD, numconn));

  {
    Context::CurrCluster().addWorkers(2);
    Fibre a1(Context::CurrCluster());
    a1.run(servaccept);
    if (numaccept == 2) {
      EventScope* es2 = Context::CurrEventScope().clone(servaccept2, nullptr);
      std::cout << "waiting for 2nd accept loop" << std::endl;
      es2->join();
    } else {
      Context::CurrCluster().addWorkers(2);
    }
    std::cout << "waiting for 1st accept loop" << std::endl;
  }

  // close server socket
  SYSCALL(lfClose(servFD));
}

void clientconn(sockaddr_in* server) {
  // create client socket and connect to server
  int fd = SYSCALLIO(lfSocket(AF_INET, SOCK_STREAM, 0));
//  std::cout << "connecting to " << inet_ntoa(server->sin_addr) << ':' << ntohs(server->sin_port) << std::endl;
  SYSCALL(lfConnect(fd, (sockaddr*)server, sizeof(sockaddr_in)));

  struct sockaddr_in local;
  socklen_t addrlen = sizeof(local);
  SYSCALL(getsockname(fd, (sockaddr*)&local, &addrlen));
//  std::cout << "connected from " << inet_ntoa(local.sin_addr) << ':' << ntohs(local.sin_port) << std::endl;

  char code = 0;
  int32_t bcount;
  bcount = random() % 8192;
  // send random-length string of random bytes, compute XOR code
  for (int i = 0; i < bcount; i += 1) {
    char buf = random() % 100;
    code = code ^ buf;
    SYSCALL_EQ(lfSend(fd, (const void*)&buf, sizeof(buf), 0), sizeof(buf));
  }
  // send terminal character
  char buf = 127;
  SYSCALL_EQ(lfSend(fd, (const void*)&buf, sizeof(buf), 0), sizeof(buf));
  std::cout << ntohs(local.sin_port) << " done sending" << std::endl;

  // receive respone
  SYSCALL_EQ(lfRecv(fd, (void*)&buf, sizeof(buf), 0), sizeof(buf));
  std::cout << ntohs(local.sin_port) << " done receiving" << std::endl;

  // check code
  if (buf == code) __atomic_add_fetch(&clntcount, 1, __ATOMIC_RELAXED);

  // close socket and terminate fibre
  SYSCALL(lfClose(fd));
}

void clientmain(sockaddr_in& addr) {
  // create more system threads
  Context::CurrCluster().addWorkers(4);
  // create and run N client fibres
  Fibre** f = new Fibre*[numconn];
  for (int n = 0; n < numconn; n += 1) f[n] = (new Fibre)->run(clientconn, &addr);
  for (int n = 0; n < numconn; n += 1) delete f[n];
  if (clntcount == numconn) std::cout << "test successfully completed" << std::endl;
  delete [] f;
}

int main(int argc, char** argv) {
#if defined(__FreeBSD__)
  sockaddr_in addr = { sizeof(sockaddr_in), AF_INET, htons(8888), { INADDR_ANY }, { 0 } };
#else // __linux__ below
  sockaddr_in addr = { AF_INET, htons(8888), { INADDR_ANY }, { 0 } };
#endif
  opts(argc, argv, addr);
  FibreInit();
  if (server) servmain(addr);
  else clientmain(addr);
  return 0;
}
