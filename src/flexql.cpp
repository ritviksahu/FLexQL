#include "flexql.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>

struct FlexQL {
    int sock = -1;
};

namespace {

#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

bool send_all(int sock, const char *data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t rc = send(sock, data + sent, len - sent, kSendFlags);
        if (rc <= 0) {
            return false;
        }
        sent += static_cast<size_t>(rc);
    }
    return true;
}

void set_error_message(char **errmsg, const std::string &message) {
    if (!errmsg) {
        return;
    }

    char *buffer = static_cast<char *>(std::malloc(message.size() + 1));
    if (!buffer) {
        return;
    }

    std::memcpy(buffer, message.c_str(), message.size() + 1);
    *errmsg = buffer;
}

bool parse_length_prefixed_token(const std::string &text, size_t &pos, std::string &out) {
    const size_t colon = text.find(':', pos);
    if (colon == std::string::npos || colon == pos) {
        return false;
    }

    for (size_t i = pos; i < colon; ++i) {
        if (text[i] < '0' || text[i] > '9') {
            return false;
        }
    }

    size_t token_length = 0;
    try {
        token_length = std::stoul(text.substr(pos, colon - pos));
    } catch (...) {
        return false;
    }

    const size_t start = colon + 1;
    if (start + token_length > text.size()) {
        return false;
    }

    out.assign(text, start, token_length);
    pos = start + token_length;
    return true;
}

bool parse_row_payload(
    const std::string &payload,
    std::vector<std::string> &values,
    std::vector<std::string> &column_names
) {
    values.clear();
    column_names.clear();

    const size_t separator = payload.find(' ');
    if (separator == std::string::npos || separator == 0) {
        return false;
    }

    for (size_t i = 0; i < separator; ++i) {
        if (payload[i] < '0' || payload[i] > '9') {
            return false;
        }
    }

    int column_count = 0;
    try {
        column_count = std::stoi(payload.substr(0, separator));
    } catch (...) {
        return false;
    }

    if (column_count < 0) {
        return false;
    }

    size_t pos = separator + 1;
    values.reserve(static_cast<size_t>(column_count));
    column_names.reserve(static_cast<size_t>(column_count));

    for (int i = 0; i < column_count; ++i) {
        std::string column_name;
        std::string value;
        if (!parse_length_prefixed_token(payload, pos, column_name)) {
            return false;
        }
        if (!parse_length_prefixed_token(payload, pos, value)) {
            return false;
        }
        column_names.push_back(column_name);
        values.push_back(value);
    }

    return pos == payload.size();
}

}  // namespace

int flexql_open(const char *host, int port, FlexQL **out_db) {
    if (!host || !out_db) {
        return FLEXQL_ERROR;
    }

    FlexQL *db = static_cast<FlexQL *>(std::malloc(sizeof(FlexQL)));
    if (!db) {
        return FLEXQL_ERROR;
    }
    db->sock = -1;

    db->sock = socket(AF_INET, SOCK_STREAM, 0);
    if (db->sock < 0) {
        std::free(db);
        return FLEXQL_ERROR;
    }

    sockaddr_in server_address {};
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(static_cast<uint16_t>(port));

    if (inet_pton(AF_INET, host, &server_address.sin_addr) != 1) {
        close(db->sock);
        std::free(db);
        return FLEXQL_ERROR;
    }

    if (connect(db->sock, reinterpret_cast<sockaddr *>(&server_address), sizeof(server_address)) < 0) {
        close(db->sock);
        std::free(db);
        return FLEXQL_ERROR;
    }

#ifndef MSG_NOSIGNAL
    int one = 1;
    setsockopt(db->sock, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

    *out_db = db;
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    if (!db) {
        return FLEXQL_OK;
    }

    if (db->sock >= 0) {
        close(db->sock);
    }
    std::free(db);
    return FLEXQL_OK;
}

int flexql_exec(
    FlexQL *db,
    const char *sql,
    int (*callback)(void *, int, char **, char **),
    void *arg,
    char **errmsg
) {
    if (errmsg) {
        *errmsg = nullptr;
    }

    if (!db || db->sock < 0 || !sql) {
        set_error_message(errmsg, "invalid FlexQL handle or SQL");
        return FLEXQL_ERROR;
    }

    const std::string request(sql);
    if (!send_all(db->sock, request.c_str(), request.size())) {
        set_error_message(errmsg, "send failed");
        return FLEXQL_ERROR;
    }

    std::string pending;
    char buffer[4096];
    bool done = false;
    bool has_error = false;
    std::string error_line;

    while (!done) {
        const ssize_t bytes_read = read(db->sock, buffer, sizeof(buffer));
        if (bytes_read < 0) {
            if (errno == EINTR) {
                continue;
            }
            set_error_message(errmsg, "read failed");
            return FLEXQL_ERROR;
        }
        if (bytes_read == 0) {
            set_error_message(errmsg, "connection closed before END");
            return FLEXQL_ERROR;
        }

        pending.append(buffer, static_cast<size_t>(bytes_read));

        size_t newline_pos = std::string::npos;
        while ((newline_pos = pending.find('\n')) != std::string::npos) {
            std::string line = pending.substr(0, newline_pos);
            pending.erase(0, newline_pos + 1);

            if (line == "END") {
                done = true;
                break;
            }

            if (line.rfind("ERROR:", 0) == 0) {
                has_error = true;
                error_line = line;
                continue;
            }

            if (callback && line.rfind("ROW ", 0) == 0) {
                std::vector<std::string> values;
                std::vector<std::string> column_names;
                const std::string payload = line.substr(4);

                if (!parse_row_payload(payload, values, column_names)) {
                    set_error_message(errmsg, "invalid row payload from server");
                    return FLEXQL_ERROR;
                }

                std::vector<char *> value_ptrs(values.size(), nullptr);
                std::vector<char *> column_ptrs(column_names.size(), nullptr);
                for (size_t i = 0; i < values.size(); ++i) {
                    value_ptrs[i] = values[i].empty() ? const_cast<char *>("") : values[i].data();
                    column_ptrs[i] = column_names[i].empty() ? const_cast<char *>("") : column_names[i].data();
                }

                if (callback(arg, static_cast<int>(value_ptrs.size()), value_ptrs.data(), column_ptrs.data()) != 0) {
                    set_error_message(errmsg, "callback requested abort");
                    return FLEXQL_ERROR;
                }
            }
        }
    }

    if (has_error) {
        set_error_message(errmsg, error_line);
        return FLEXQL_ERROR;
    }

    return FLEXQL_OK;
}

void flexql_free(void *ptr) {
    std::free(ptr);
}
