#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <deque>
#include <memory>
#include <regex>
#include <thread>
#include <vector>

#include "../comm.hpp"
#include "hashmap.hpp"
#include "profile.hpp"
#include "rbv.hpp"
#include "utils.hpp"

#ifdef PROFILE_MEM
#include "profile-mem.hpp"
#endif

hashmap_t *hm_safe = nullptr;
std::string replica_ip;

thread_local int replica_fd;

const int kMaxQueueSize = 1000000;

struct packet_info {
    std::string packet;
    long long t_start;
};
thread_local packet_info packet_queue[kMaxQueueSize];

struct pending_response_t {
    int client_fd;
    std::string response;
    bool is_quit;
};

struct fd_worker {
    char *wt_buffer;
    size_t len;
    fd_reader reader;
    bool sync_validation;
    std::deque<pending_response_t> *pending;
    bool *shutdown_requested;

    fd_worker(int _fd, bool _sync_validation,
              std::deque<pending_response_t> *_pending,
              bool *_shutdown_requested)
        : reader(_fd) {
        wt_buffer = (char *)malloc(kBufferSize);
        len = 0;
        sync_validation = _sync_validation;
        pending = _pending;
        shutdown_requested = _shutdown_requested;
    }

    bool run() {
        while ((len = reader.read_packet('\n'))) {
            if (sync_validation && shutdown_requested &&
                *shutdown_requested) {
                continue;
            }
            if (!memcmp(reader.packet, "quit", 4)) {
                if (!sync_validation) {
                    write_all(replica_fd, "quit\n");
                    fd_reader replica_reader(replica_fd);
                    size_t len = replica_reader.read_packet('\n');
                    assert(len > 0);
                    return true;
                }
                if (shutdown_requested && !*shutdown_requested) {
                    write_all(replica_fd, "quit\n");
                    pending->push_back(pending_response_t{-1, "", true});
                    *shutdown_requested = true;
                }
                continue;
            }
            char *packet = reader.packet;
            char *cmd = packet;
            size_t cmd_len = len;
            uint32_t unused_crc = 0;
            (void)consume_crc_prefix(cmd, cmd_len, unused_crc);
            long long t_start = profile::get_us_abs();

            if (cmd[0] == 's') {  // set
                Key key;
                Val val;
                memcpy(key.ch, cmd + 4, KEY_LEN);
                memcpy(val.ch, cmd + 4 + KEY_LEN + 1, VAL_LEN);
                RetType ret = hashmap_set(hm_safe, key, val);
                memcpy(wt_buffer, kRetVals[ret], strlen(kRetVals[ret]) + 1);
            } else if (cmd[0] == 'g') {  // get
                Key key;
                memcpy(key.ch, cmd + 4, KEY_LEN);
                const Val *val = hashmap_get(hm_safe, key);
                if (val != nullptr) {
                    std::string ans = kRetVals[kValue];
                    ans += std::string(val->ch, VAL_LEN);
                    ans += kCrlf;
                    memcpy(wt_buffer, ans.data(), ans.size());
                    wt_buffer[ans.size()] = '\0';
                } else {
                    memcpy(wt_buffer, kRetVals[kNotFound],
                           strlen(kRetVals[kNotFound]) + 1);
                }
            } else if (cmd[0] == 'd') {  // del
                Key key;
                memcpy(key.ch, cmd + 4, KEY_LEN);
                RetType ret = hashmap_del(hm_safe, key);
                memcpy(wt_buffer, kRetVals[ret], strlen(kRetVals[ret]) + 1);
            } else {
                memcpy(wt_buffer, kRetVals[kError],
                       strlen(kRetVals[kError]) + 1);
            }

            std::string p;
            p += rbv::toString20(t_start) + rbv::hasher.finalize() + "\n";
            p += std::string(packet, len);
            write_all(replica_fd, p);

            if (sync_validation) {
                pending->push_back(pending_response_t{
                    reader.fd, std::string(wt_buffer, strlen(wt_buffer)),
                    false});
            } else {
                write_all(reader.fd, wt_buffer, strlen(wt_buffer));
            }
        }
        return false;
    }
    ~fd_worker() { free(wt_buffer); }
};

void Start(int port, int replica_port, bool sync_validation) {
    const int MAX_EVENTS = 128;  // max total active connections

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(listen_fd >= 0);

    replica_fd = connect_server(replica_ip, replica_port);
    write_all(replica_fd, sync_validation ? "mode sync\n" : "mode async\n");

    struct sockaddr_in server_addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr = {.s_addr = INADDR_ANY},
    };
    if (bind(listen_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) <
        0) {
        close(listen_fd);
        assert("bind error" && false);
    }
    if (listen(listen_fd, 1) < 0) {
        close(listen_fd);
        assert("listen error" && false);
    }
    printf("server listening on port %d\n", port);

    int efd = epoll_create1(0);
    if (efd == -1) {
        assert("epoll create error" && false);
    }

    struct epoll_event ev, events[MAX_EVENTS];
    auto epoll_init = [&](int fd) {
        fcntl(fd, F_SETFL, O_NONBLOCK);
        ev.data.fd = fd;
        ev.events = EPOLLIN | EPOLLET;
        if (epoll_ctl(efd, EPOLL_CTL_ADD, fd, &ev) == -1) {
            assert("epoll ctl error" && false);
        }
    };
    epoll_init(listen_fd);
    if (sync_validation) {
        epoll_init(replica_fd);
    }

    std::map<int, std::unique_ptr<fd_worker>> workers;
    std::deque<pending_response_t> pending_responses;
    bool shutdown_requested = false;
    fd_reader replica_reader(replica_fd);

    int timeout = -1;

    while (true) {
        int nfds = epoll_wait(efd, events, MAX_EVENTS, timeout);
        if (nfds == -1) {
            if (errno == EINTR) {
                // avoid the interrupt caused by strace
                continue;
            }
            assert("epoll wait error" && false);
        }
        if (nfds == 0) {
            fprintf(stderr, "server stopped due to inactivity.\n");
            break;
        }
        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;
            uint32_t state = events[i].events;
            if (fd == listen_fd) {
                // new client connection
                struct sockaddr_in client_addr;
                socklen_t client_addr_len = sizeof(client_addr);
                while (true) {
                    int conn_fd =
                        accept(listen_fd, (struct sockaddr *)&client_addr,
                               &client_addr_len);
                    if (conn_fd == -1) {
                        assert((errno == EAGAIN) || (errno == EWOULDBLOCK));
                        break;
                    }
                    epoll_init(conn_fd);
                    workers[conn_fd] = std::make_unique<fd_worker>(
                        conn_fd, sync_validation, &pending_responses,
                        &shutdown_requested);
                }
            } else if (sync_validation && fd == replica_fd) {
                while (true) {
                    size_t ack_len = replica_reader.read_packet('\n');
                    if (ack_len == 0) break;
                    assert(ack_len >= 3);
                    assert(!memcmp(replica_reader.packet, "ACK", 3));

                    if (pending_responses.empty()) continue;
                    pending_response_t item =
                        std::move(pending_responses.front());
                    pending_responses.pop_front();
                    if (item.is_quit) {
                        return;
                    }
                    if (workers.find(item.client_fd) != workers.end()) {
                        write_all(item.client_fd, item.response);
                    }
                }
            } else if ((state & (EPOLLERR | EPOLLHUP)) && !(state & EPOLLIN)) {
                // client connection closed
                close(fd);
                workers.erase(fd);
            } else {
                // receive message on this client socket
                auto it = workers.find(fd);
                if (it == workers.end()) continue;
                if (it->second->run()) return;
            }
        }
    }
}

int main(int argc, char *argv[]) {
    bool sync_validation = false;
    std::vector<std::string> positional;
    positional.reserve(static_cast<size_t>(argc));
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--sync") {
            sync_validation = true;
            continue;
        }
        if (arg == "--async") {
            sync_validation = false;
            continue;
        }
        positional.emplace_back(std::move(arg));
    }
    if (positional.empty() || positional.size() > 4) {
        fprintf(stderr,
                "Usage: %s <port> [ngroups] [replica-port] [replica-ip] "
                "[--sync|--async]\n",
                argv[0]);
        fprintf(stderr,
                "Default values: ngroups=3, replica-port=6789, "
                "replica-ip=localhost, validation=async\n");
        return 1;
    }
#ifdef PROFILE_MEM
    profile::mem::init_mem("memcached-memory_status-rbv-primary.log");
    profile::mem::start();
#endif
    int port = atoi(positional[0].c_str());
    int ngroups = 3;
    if (positional.size() >= 2) ngroups = atoi(positional[1].c_str());
    int replica_port = 6789;
    if (positional.size() >= 3) replica_port = atoi(positional[2].c_str());
    replica_ip = "localhost";
    if (positional.size() >= 4) replica_ip = positional[3];
    hm_safe = hashmap_t::make(1 << 24);
    std::vector<std::thread> threads;
    for (int i = 0; i < ngroups; ++i) {
        threads.emplace_back(Start, port + i, replica_port + i,
                             sync_validation);
    }
    for (auto &thread : threads) {
        thread.join();
    }
#ifdef PROFILE_MEM
    profile::mem::stop();
#endif
    return 0;
}
