#include "rdma/exchange_msg.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/ip.h>

#include <cstdlib>

#include "utils/debug.hpp"

namespace Beehive {

namespace tcp {

sockaddr_in get_sock_addr(const char *server_addr, const char *server_port) {
    in_addr_t addr = inet_addr(server_addr);
    ASSERT(addr != -1);
    in_port_t port = atoi(server_port);
    ASSERT(port != 0);
    sockaddr_in sock_addr = {
        .sin_family = AF_INET, .sin_port = port, .sin_addr = {.s_addr = addr}};
    return sock_addr;
}

int listen_for_client(const char *server_addr, const char *server_port,
                      int &sock_fd) {
    sockaddr_in sock_addr = get_sock_addr(server_addr, server_port);
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT(sock_fd);

    int on = 1;
    CHECK_ERR(setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)));

    CHECK_ERR(bind(sock_fd, reinterpret_cast<sockaddr *>(&sock_addr),
                   sizeof(sock_addr)));

    CHECK_ERR(listen(sock_fd, 1));

    sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    int conn_fd = accept(sock_fd, reinterpret_cast<sockaddr *>(&client_addr),
                         &client_addr_len);
    ASSERT(conn_fd);
    ASSERT(client_addr_len == sizeof(client_addr));
    return conn_fd;
}

int connect_to_server(const char *server_addr, const char *server_port) {
    sockaddr_in sock_addr = get_sock_addr(server_addr, server_port);
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT(sock_fd);
    CHECK_ERR(connect(sock_fd, reinterpret_cast<sockaddr *>(&sock_addr),
                      sizeof(sock_addr)));
    return sock_fd;
}

}  // namespace tcp

}  // namespace Beehive
