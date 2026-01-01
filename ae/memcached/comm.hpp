#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <regex>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "utils.hpp"

static inline void write_all(int fd, const char *buf, size_t len) {
    size_t written = 0;
    while (written < len) {
        ssize_t ret = write(fd, buf + written, len - written);
        assert(ret > 0);
        written += ret;
    }
}

static inline void write_all(int fd, std::string s) {
    size_t written = 0, len = s.size();
    while (written < len) {
        ssize_t ret = write(fd, s.data() + written, len - written);
        assert(ret > 0);
        written += ret;
    }
}

static constexpr size_t kBufferSize = 1 << 14, kMaxCmdLen = 1 << 10;

struct fd_reader {
    int fd;
    char *rd_buffer, *packet;
    size_t rx_bytes, cur_pos;
    fd_reader(int _fd) {
        fd = _fd;
        rd_buffer = (char *)malloc(kBufferSize);
        packet = nullptr;
        rx_bytes = cur_pos = 0;
    }
    ~fd_reader() { free(rd_buffer); }
    int read_from_socket() {
        ssize_t ret = read(fd, rd_buffer + rx_bytes, kBufferSize - rx_bytes);
        if (ret <= 0) {
            return -1;
        }
        rx_bytes += ret;
        return 0;
    }
    int read_packet(char delim = '\n') {
        if (cur_pos >= kBufferSize || rx_bytes == 0) {
            cur_pos = 0;
            if (read_from_socket() < 0) {
                if (errno == 0) {
                    fprintf(stderr, "client closed the connection\n");
                    exit(0);
                }
                assert((errno == EAGAIN) || (errno == EWOULDBLOCK));
                return 0;
            }
        }
        void *end = memchr(rd_buffer + cur_pos, delim, rx_bytes);
        if (!end) {
            assert(rx_bytes <= kMaxCmdLen);
            memcpy(rd_buffer, rd_buffer + cur_pos, rx_bytes);
            cur_pos = 0;
            if (read_from_socket() < 0) {
                assert((errno == EAGAIN) || (errno == EWOULDBLOCK));
                return 0;
            }
            end = memchr(rd_buffer + cur_pos, delim, rx_bytes);
        }
        if (!end) {
            return 0;
        }
        packet = rd_buffer + cur_pos;
        size_t len = (char *)end - (rd_buffer + cur_pos) + 1;
        cur_pos += len;
        rx_bytes -= len;
        return len;
    }
};

// If the packet starts with "<digits>#", consume the prefix and return the CRC.
// This is used to pass a sender-computed CRC into libsei's __begin().
static inline bool consume_crc_prefix(char *&packet, size_t &len,
                                      uint32_t &crc_out) {
    if (len == 0) return false;
    if (packet[0] < '0' || packet[0] > '9') return false;

    void *hash_pos = memchr(packet, '#', len);
    if (hash_pos == nullptr) return false;

    uint64_t crc = 0;
    for (char *p = packet; p < hash_pos; ++p) {
        const char ch = *p;
        if (ch < '0' || ch > '9') return false;
        crc = crc * 10 + static_cast<uint64_t>(ch - '0');
        if (crc > UINT32_MAX) return false;
    }

    const size_t prefix_len =
        static_cast<size_t>(static_cast<char *>(hash_pos) - packet) + 1;
    packet = static_cast<char *>(hash_pos) + 1;
    len -= prefix_len;
    crc_out = static_cast<uint32_t>(crc);
    return true;
}

static inline int connect_server(std::string ip, int port) {
    struct sockaddr_in server_addr;
    int fd;

    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf(" create socket error!\n ");
        exit(1);
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    inet_aton(ip.c_str(), &server_addr.sin_addr);
    server_addr.sin_port = htons(port);

    if (connect(fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        assert(false);
    }

    my_usleep(1000);
    return fd;
}
