#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <cctype>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr int kPort = 9000;
constexpr int kBacklog = 32;
constexpr size_t kReadBufferSize = 4096;
constexpr size_t kCheckpointInsertOps = 32768;
constexpr const char *kDataDir = "dbdata";
constexpr const char *kJournalPath = "dbdata/journal.txt";
constexpr const char *kInsertWalPath = "dbdata/insert.wal";

#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

std::atomic<bool> g_running{true};
std::mutex g_log_mutex;

enum class TokenKind {
    Identifier,
    Number,
    String,
    Symbol
};

struct Token {
    TokenKind kind;
    std::string text;
};

struct Column {
    std::string name;
    std::string type;
};

struct Row {
    long long row_id = 0;
    std::vector<std::string> values;
};

struct Table {
    std::string name;
    std::vector<Column> columns;
    std::unordered_map<std::string, size_t> column_index;
    std::vector<Row> rows;
    std::vector<std::unordered_map<std::string, std::vector<size_t>>> equality_indexes;
    std::vector<bool> equality_index_ready;
    size_t flushed_rows = 0;
    long long next_row_id = 1;
    int data_fd = -1;
};

struct ColumnRef {
    std::string table;
    std::string column;
};

struct Operand {
    bool is_column = false;
    ColumnRef column;
    std::string literal;
};

struct Condition {
    Operand left;
    std::string op;
    Operand right;
};

struct JoinClause {
    std::string table_name;
    Condition on;
};

struct Projection {
    bool is_star = false;
    ColumnRef column;
};

struct CreateTableStatement {
    std::string table_name;
    std::vector<Column> columns;
};

struct DropTableStatement {
    bool if_exists = false;
    std::string table_name;
};

struct InsertStatement {
    std::string table_name;
    std::vector<std::vector<std::string>> rows;
};

struct SelectStatement {
    bool count_star = false;
    std::vector<Projection> projections;
    std::string from_table;
    std::optional<JoinClause> join;
    std::vector<Condition> where_conditions;
};

struct QueryRow {
    std::vector<std::string> column_names;
    std::vector<std::string> values;
};

struct QueryResult {
    bool is_error = false;
    std::string error;
    std::vector<QueryRow> rows;
};

struct PreparedInsert {
    std::string table_name;
    std::vector<Row> rows;
};

struct JournalEntry {
    std::string type;
    std::string table_name;
    std::vector<Column> columns;
    std::vector<Row> rows;
};

std::string to_upper(const std::string &value) {
    std::string out = value;
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
        return static_cast<char>(std::toupper(c));
    });
    return out;
}

std::string encode_token(const std::string &text) {
    return std::to_string(text.size()) + ":" + text;
}

bool parse_length_prefixed_token(const std::string &text, size_t &pos, std::string &out) {
    const size_t colon = text.find(':', pos);
    if (colon == std::string::npos || colon == pos) {
        return false;
    }

    size_t len = 0;
    const auto parse_result = std::from_chars(text.data() + pos, text.data() + colon, len);
    if (parse_result.ec != std::errc()) {
        return false;
    }

    const size_t start = colon + 1;
    if (start + len > text.size()) {
        return false;
    }

    out.assign(text, start, len);
    pos = start + len;
    return true;
}

std::string serialize_row(const Row &row) {
    std::string line = std::to_string(row.row_id);
    line += " ";
    line += std::to_string(row.values.size());
    for (const auto &value : row.values) {
        line += " ";
        line += encode_token(value);
    }
    return line;
}

bool parse_row_line(const std::string &line, Row &row) {
    size_t pos = 0;
    size_t space = line.find(' ', pos);
    if (space == std::string::npos) {
        return false;
    }

    row.row_id = std::stoll(line.substr(pos, space - pos));
    pos = space + 1;
    space = line.find(' ', pos);
    const std::string count_text = (space == std::string::npos) ? line.substr(pos) : line.substr(pos, space - pos);
    const size_t value_count = static_cast<size_t>(std::stoull(count_text));
    pos = (space == std::string::npos) ? line.size() : space + 1;

    row.values.clear();
    row.values.reserve(value_count);
    for (size_t i = 0; i < value_count; ++i) {
        std::string value;
        if (!parse_length_prefixed_token(line, pos, value)) {
            return false;
        }
        row.values.push_back(std::move(value));
        if (pos < line.size() && line[pos] == ' ') {
            pos++;
        }
    }
    return true;
}

bool write_all_fd(int fd, const std::string &data) {
    size_t written = 0;
    while (written < data.size()) {
        const ssize_t rc = ::write(fd, data.data() + written, data.size() - written);
        if (rc <= 0) {
            return false;
        }
        written += static_cast<size_t>(rc);
    }
    return true;
}

bool write_all_fd(int fd, const void *data, size_t len) {
    size_t written = 0;
    const char *ptr = static_cast<const char *>(data);
    while (written < len) {
        const ssize_t rc = ::write(fd, ptr + written, len - written);
        if (rc <= 0) {
            return false;
        }
        written += static_cast<size_t>(rc);
    }
    return true;
}

bool read_exact(std::istream &input, char *buffer, size_t len) {
    input.read(buffer, static_cast<std::streamsize>(len));
    return static_cast<size_t>(input.gcount()) == len;
}

bool write_u32(int fd, uint32_t value) {
    return write_all_fd(fd, &value, sizeof(value));
}

bool write_u64(int fd, uint64_t value) {
    return write_all_fd(fd, &value, sizeof(value));
}

bool read_u32(std::istream &input, uint32_t &value) {
    return read_exact(input, reinterpret_cast<char *>(&value), sizeof(value));
}

bool read_u64(std::istream &input, uint64_t &value) {
    return read_exact(input, reinterpret_cast<char *>(&value), sizeof(value));
}

bool sync_file_fd(int fd) {
#if defined(_POSIX_SYNCHRONIZED_IO) && _POSIX_SYNCHRONIZED_IO > 0
    return ::fdatasync(fd) == 0;
#else
    return ::fsync(fd) == 0;
#endif
}

bool write_binary_string(int fd, const std::string &value) {
    return write_u32(fd, static_cast<uint32_t>(value.size())) &&
           write_all_fd(fd, value.data(), value.size());
}

void append_bytes(std::string &buffer, const void *data, size_t len) {
    buffer.append(static_cast<const char *>(data), len);
}

void append_u32(std::string &buffer, uint32_t value) {
    append_bytes(buffer, &value, sizeof(value));
}

void append_u64(std::string &buffer, uint64_t value) {
    append_bytes(buffer, &value, sizeof(value));
}

void append_binary_string(std::string &buffer, const std::string &value) {
    append_u32(buffer, static_cast<uint32_t>(value.size()));
    buffer.append(value);
}

void append_binary_row(std::string &buffer, const Row &row) {
    append_u64(buffer, static_cast<uint64_t>(row.row_id));
    append_u32(buffer, static_cast<uint32_t>(row.values.size()));
    for (const auto &value : row.values) {
        append_binary_string(buffer, value);
    }
}

bool read_binary_string(std::istream &input, std::string &value) {
    uint32_t len = 0;
    if (!read_u32(input, len)) {
        return false;
    }
    value.resize(len);
    return len == 0 || read_exact(input, value.data(), len);
}

bool read_binary_row(std::istream &input, Row &row) {
    uint64_t row_id = 0;
    uint32_t value_count = 0;
    if (!read_u64(input, row_id)) {
        return false;
    }
    if (!read_u32(input, value_count)) {
        return false;
    }
    row.row_id = static_cast<long long>(row_id);
    row.values.clear();
    row.values.reserve(value_count);
    for (uint32_t i = 0; i < value_count; ++i) {
        std::string value;
        if (!read_binary_string(input, value)) {
            return false;
        }
        row.values.push_back(std::move(value));
    }
    return true;
}

bool fsync_directory(const fs::path &path) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return false;
    }
    const bool ok = ::fsync(fd) == 0;
    ::close(fd);
    return ok;
}

std::vector<Token> tokenize(const std::string &sql, std::string &error) {
    std::vector<Token> tokens;
    for (size_t i = 0; i < sql.size();) {
        const char c = sql[i];
        if (std::isspace(static_cast<unsigned char>(c))) {
            i++;
            continue;
        }

        if (std::isalpha(static_cast<unsigned char>(c)) || c == '_') {
            size_t start = i++;
            while (i < sql.size() && (std::isalnum(static_cast<unsigned char>(sql[i])) || sql[i] == '_')) {
                i++;
            }
            tokens.push_back({TokenKind::Identifier, sql.substr(start, i - start)});
            continue;
        }

        if (std::isdigit(static_cast<unsigned char>(c)) || (c == '-' && i + 1 < sql.size() && std::isdigit(static_cast<unsigned char>(sql[i + 1])))) {
            size_t start = i++;
            while (i < sql.size() && (std::isdigit(static_cast<unsigned char>(sql[i])) || sql[i] == '.')) {
                i++;
            }
            tokens.push_back({TokenKind::Number, sql.substr(start, i - start)});
            continue;
        }

        if (c == '\'') {
            i++;
            std::string value;
            while (i < sql.size()) {
                if (sql[i] == '\'') {
                    if (i + 1 < sql.size() && sql[i + 1] == '\'') {
                        value.push_back('\'');
                        i += 2;
                        continue;
                    }
                    i++;
                    break;
                }
                value.push_back(sql[i++]);
            }
            tokens.push_back({TokenKind::String, value});
            continue;
        }

        if ((c == '>' || c == '<') && i + 1 < sql.size() && sql[i + 1] == '=') {
            tokens.push_back({TokenKind::Symbol, sql.substr(i, 2)});
            i += 2;
            continue;
        }

        if (std::string("(),;*=.<>").find(c) != std::string::npos) {
            tokens.push_back({TokenKind::Symbol, sql.substr(i, 1)});
            i++;
            continue;
        }

        error = "unexpected character in SQL";
        return {};
    }
    return tokens;
}

bool parse_insert_fast(const std::string &sql, InsertStatement &statement, std::string &error) {
    auto skip_ws = [&](size_t &pos) {
        while (pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[pos]))) {
            pos++;
        }
    };

    auto consume_keyword = [&](size_t &pos, const std::string &keyword) -> bool {
        skip_ws(pos);
        if (pos + keyword.size() > sql.size()) {
            return false;
        }
        for (size_t i = 0; i < keyword.size(); ++i) {
            if (std::toupper(static_cast<unsigned char>(sql[pos + i])) != keyword[i]) {
                return false;
            }
        }
        pos += keyword.size();
        return true;
    };

    auto parse_identifier = [&](size_t &pos, std::string &out) -> bool {
        skip_ws(pos);
        size_t start = pos;
        if (pos >= sql.size() || !(std::isalpha(static_cast<unsigned char>(sql[pos])) || sql[pos] == '_')) {
            return false;
        }
        pos++;
        while (pos < sql.size() && (std::isalnum(static_cast<unsigned char>(sql[pos])) || sql[pos] == '_')) {
            pos++;
        }
        out = to_upper(sql.substr(start, pos - start));
        return true;
    };

    auto parse_value = [&](size_t &pos, std::string &out) -> bool {
        skip_ws(pos);
        if (pos >= sql.size()) {
            return false;
        }
        if (sql[pos] == '\'') {
            pos++;
            out.clear();
            while (pos < sql.size()) {
                if (sql[pos] == '\'') {
                    if (pos + 1 < sql.size() && sql[pos + 1] == '\'') {
                        out.push_back('\'');
                        pos += 2;
                        continue;
                    }
                    pos++;
                    return true;
                }
                out.push_back(sql[pos++]);
            }
            return false;
        }

        size_t start = pos;
        while (pos < sql.size() && !std::isspace(static_cast<unsigned char>(sql[pos])) && sql[pos] != ',' && sql[pos] != ')' && sql[pos] != ';') {
            pos++;
        }
        if (start == pos) {
            return false;
        }
        out = sql.substr(start, pos - start);
        return true;
    };

    size_t pos = 0;
    if (!consume_keyword(pos, "INSERT") || !consume_keyword(pos, "INTO")) {
        error = "expected INSERT INTO";
        return false;
    }
    if (!parse_identifier(pos, statement.table_name)) {
        error = "expected table name";
        return false;
    }
    if (!consume_keyword(pos, "VALUES")) {
        error = "expected VALUES";
        return false;
    }

    size_t estimated_rows = 0;
    bool in_string = false;
    for (size_t i = pos; i < sql.size(); ++i) {
        if (sql[i] == '\'') {
            if (in_string && i + 1 < sql.size() && sql[i + 1] == '\'') {
                i++;
                continue;
            }
            in_string = !in_string;
        } else if (!in_string && sql[i] == '(') {
            estimated_rows++;
        }
    }
    if (estimated_rows > 0) {
        statement.rows.reserve(estimated_rows);
    }

    while (true) {
        skip_ws(pos);
        if (pos >= sql.size() || sql[pos] != '(') {
            error = "expected (";
            return false;
        }
        pos++;

        std::vector<std::string> row;
        while (true) {
            std::string value;
            if (!parse_value(pos, value)) {
                error = "expected value";
                return false;
            }
            row.push_back(std::move(value));
            skip_ws(pos);
            if (pos < sql.size() && sql[pos] == ',') {
                pos++;
                skip_ws(pos);
                if (pos < sql.size() && sql[pos] != ')') {
                    continue;
                }
            }
            break;
        }

        skip_ws(pos);
        if (pos >= sql.size() || sql[pos] != ')') {
            error = "expected )";
            return false;
        }
        pos++;
        statement.rows.push_back(std::move(row));

        skip_ws(pos);
        if (pos < sql.size() && sql[pos] == ',') {
            pos++;
            continue;
        }
        break;
    }

    skip_ws(pos);
    if (pos < sql.size() && sql[pos] == ';') {
        pos++;
    }
    skip_ws(pos);
    if (pos != sql.size()) {
        error = "unexpected tokens after INSERT";
        return false;
    }
    return true;
}

std::string first_keyword(const std::string &sql) {
    size_t pos = 0;
    while (pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[pos]))) {
        pos++;
    }
    size_t start = pos;
    while (pos < sql.size() && (std::isalpha(static_cast<unsigned char>(sql[pos])) || sql[pos] == '_')) {
        pos++;
    }
    return to_upper(sql.substr(start, pos - start));
}

class Parser {
public:
    explicit Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

    std::optional<CreateTableStatement> parse_create_table(std::string &error) {
        if (!match_keyword("CREATE") || !match_keyword("TABLE")) {
            error = "expected CREATE TABLE";
            return std::nullopt;
        }

        CreateTableStatement statement;
        if (!parse_identifier(statement.table_name, error) || !match_symbol("(")) {
            if (error.empty()) {
                error = "invalid CREATE TABLE syntax";
            }
            return std::nullopt;
        }

        while (true) {
            Column column;
            if (!parse_identifier(column.name, error)) {
                return std::nullopt;
            }

            int paren_depth = 0;
            while (!at_end()) {
                const Token &token = peek();
                if (token.text == "(") {
                    paren_depth++;
                } else if (token.text == ")") {
                    if (paren_depth == 0) {
                        break;
                    }
                    paren_depth--;
                } else if (token.text == "," && paren_depth == 0) {
                    break;
                }

                if (!column.type.empty()) {
                    const bool no_space_before = (token.text == ")" || token.text == "," || token.text == "(");
                    const bool no_space_after_open = (!column.type.empty() && column.type.back() == '(');
                    if (!no_space_before && !no_space_after_open) {
                        column.type += " ";
                    }
                }
                column.type += token.text;
                advance();
            }

            if (column.type.empty()) {
                error = "missing column type";
                return std::nullopt;
            }

            statement.columns.push_back(std::move(column));
            if (match_symbol(",")) {
                continue;
            }
            break;
        }

        if (!match_symbol(")")) {
            error = "expected )";
            return std::nullopt;
        }
        match_symbol(";");
        if (!at_end()) {
            error = "unexpected tokens after CREATE TABLE";
            return std::nullopt;
        }
        return statement;
    }

    std::optional<DropTableStatement> parse_drop_table(std::string &error) {
        if (!match_keyword("DROP") || !match_keyword("TABLE")) {
            error = "expected DROP TABLE";
            return std::nullopt;
        }

        DropTableStatement statement;
        if (match_keyword("IF")) {
            if (!match_keyword("EXISTS")) {
                error = "expected EXISTS after IF";
                return std::nullopt;
            }
            statement.if_exists = true;
        }

        if (!parse_identifier(statement.table_name, error)) {
            return std::nullopt;
        }

        match_symbol(";");
        if (!at_end()) {
            error = "unexpected tokens after DROP TABLE";
            return std::nullopt;
        }
        return statement;
    }

    std::optional<InsertStatement> parse_insert(std::string &error) {
        if (!match_keyword("INSERT") || !match_keyword("INTO")) {
            error = "expected INSERT INTO";
            return std::nullopt;
        }

        InsertStatement statement;
        if (!parse_identifier(statement.table_name, error) || !match_keyword("VALUES")) {
            if (error.empty()) {
                error = "invalid INSERT syntax";
            }
            return std::nullopt;
        }

        do {
            if (!match_symbol("(")) {
                error = "expected (";
                return std::nullopt;
            }

            std::vector<std::string> row;
            while (true) {
                std::string value;
                if (!parse_value(value, error)) {
                    return std::nullopt;
                }
                row.push_back(std::move(value));
                if (match_symbol(",")) {
                    continue;
                }
                break;
            }

            if (!match_symbol(")")) {
                error = "expected )";
                return std::nullopt;
            }
            statement.rows.push_back(std::move(row));
        } while (match_symbol(","));

        match_symbol(";");
        if (!at_end()) {
            error = "unexpected tokens after INSERT";
            return std::nullopt;
        }
        return statement;
    }

    std::optional<SelectStatement> parse_select(std::string &error) {
        if (!match_keyword("SELECT")) {
            error = "expected SELECT";
            return std::nullopt;
        }

        SelectStatement statement;
        if (match_keyword("COUNT")) {
            if (!match_symbol("(") || !match_symbol("*") || !match_symbol(")")) {
                error = "invalid COUNT(*) syntax";
                return std::nullopt;
            }
            statement.count_star = true;
        } else {
            while (true) {
                Projection projection;
                if (match_symbol("*")) {
                    projection.is_star = true;
                } else if (!parse_column_ref(projection.column, error)) {
                    return std::nullopt;
                }
                statement.projections.push_back(std::move(projection));
                if (match_symbol(",")) {
                    continue;
                }
                break;
            }
        }

        if (!match_keyword("FROM") || !parse_identifier(statement.from_table, error)) {
            if (error.empty()) {
                error = "invalid SELECT syntax";
            }
            return std::nullopt;
        }

        if (match_keyword("INNER")) {
            if (!match_keyword("JOIN")) {
                error = "expected JOIN after INNER";
                return std::nullopt;
            }

            JoinClause join;
            if (!parse_identifier(join.table_name, error) || !match_keyword("ON")) {
                if (error.empty()) {
                    error = "invalid JOIN syntax";
                }
                return std::nullopt;
            }
            if (!parse_condition(join.on, error, true)) {
                return std::nullopt;
            }
            statement.join = std::move(join);
        }

        if (match_keyword("WHERE")) {
            while (true) {
                Condition condition;
                if (!parse_condition(condition, error, false)) {
                    return std::nullopt;
                }
                statement.where_conditions.push_back(std::move(condition));
                if (match_keyword("AND")) {
                    continue;
                }
                break;
            }
        }

        match_symbol(";");
        if (!at_end()) {
            error = "unexpected tokens after SELECT";
            return std::nullopt;
        }
        return statement;
    }

private:
    const Token &peek() const {
        static const Token empty{TokenKind::Symbol, ""};
        return index_ < tokens_.size() ? tokens_[index_] : empty;
    }

    bool at_end() const {
        return index_ >= tokens_.size();
    }

    void advance() {
        if (!at_end()) {
            index_++;
        }
    }

    bool match_symbol(const std::string &symbol) {
        if (!at_end() && peek().text == symbol) {
            advance();
            return true;
        }
        return false;
    }

    bool match_keyword(const std::string &keyword) {
        if (at_end() || peek().kind != TokenKind::Identifier) {
            return false;
        }
        if (to_upper(peek().text) == keyword) {
            advance();
            return true;
        }
        return false;
    }

    bool parse_identifier(std::string &out, std::string &error) {
        if (at_end() || (peek().kind != TokenKind::Identifier && peek().kind != TokenKind::Number)) {
            error = "expected identifier";
            return false;
        }
        out = to_upper(peek().text);
        advance();
        return true;
    }

    bool parse_value(std::string &out, std::string &error) {
        if (at_end()) {
            error = "expected value";
            return false;
        }
        if (peek().kind == TokenKind::String || peek().kind == TokenKind::Number || peek().kind == TokenKind::Identifier) {
            out = peek().text;
            advance();
            return true;
        }
        error = "expected value";
        return false;
    }

    bool parse_column_ref(ColumnRef &out, std::string &error) {
        std::string first;
        if (!parse_identifier(first, error)) {
            return false;
        }
        if (match_symbol(".")) {
            out.table = first;
            if (!parse_identifier(out.column, error)) {
                return false;
            }
        } else {
            out.column = first;
        }
        return true;
    }

    bool parse_operand(Operand &operand, std::string &error, bool require_column_on_both_sides) {
        if (at_end()) {
            error = "expected operand";
            return false;
        }

        if (peek().kind == TokenKind::String || peek().kind == TokenKind::Number) {
            if (require_column_on_both_sides) {
                error = "JOIN operands must be column references";
                return false;
            }
            operand.is_column = false;
            operand.literal = peek().text;
            advance();
            return true;
        }

        operand.is_column = true;
        return parse_column_ref(operand.column, error);
    }

    bool parse_condition(Condition &condition, std::string &error, bool require_column_on_both_sides) {
        if (!parse_operand(condition.left, error, require_column_on_both_sides)) {
            return false;
        }

        if (at_end() || peek().kind != TokenKind::Symbol ||
            (peek().text != "=" && peek().text != ">" && peek().text != "<" && peek().text != ">=" && peek().text != "<=")) {
            error = "expected comparison operator";
            return false;
        }
        condition.op = peek().text;
        advance();

        if (!parse_operand(condition.right, error, require_column_on_both_sides)) {
            return false;
        }
        return true;
    }

    std::vector<Token> tokens_;
    size_t index_ = 0;
};

class Database {
public:
    bool initialize(std::string &error) {
        try {
            fs::create_directories(kDataDir);
        } catch (const std::exception &ex) {
            error = ex.what();
            return false;
        }

        if (!load_tables(error)) {
            return false;
        }
        if (!recover_from_journal(error)) {
            return false;
        }
        if (!recover_from_insert_wal(error)) {
            return false;
        }
        if (!ensure_wal_open(error)) {
            return false;
        }
        return true;
    }

    QueryResult execute_create_table(const CreateTableStatement &statement) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        std::string checkpoint_error;
        if (!checkpoint_locked(checkpoint_error)) {
            return make_error(checkpoint_error);
        }

        const std::string table_key = to_upper(statement.table_name);
        if (tables_.count(table_key) != 0) {
            return make_error("table " + statement.table_name + " already exists");
        }

        if (!write_create_journal(statement)) {
            return make_error("failed to write journal");
        }

        auto table = std::make_shared<Table>();
        table->name = table_key;
        table->columns = statement.columns;
        table->equality_indexes.resize(table->columns.size());
        table->equality_index_ready.assign(table->columns.size(), false);
        table->flushed_rows = 0;
        for (size_t i = 0; i < table->columns.size(); ++i) {
            table->column_index[to_upper(table->columns[i].name)] = i;
        }

        std::string error;
        if (!persist_table_metadata(*table, error) || !create_empty_data_file(table->name, error) || !open_table_data_fd(*table, error)) {
            clear_journal();
            return make_error(error);
        }

        tables_[table_key] = table;
        clear_journal();
        return {};
    }

    QueryResult execute_drop_table(const DropTableStatement &statement) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        std::string checkpoint_error;
        if (!checkpoint_locked(checkpoint_error)) {
            return make_error(checkpoint_error);
        }

        const std::string table_key = to_upper(statement.table_name);
        auto it = tables_.find(table_key);
        if (it == tables_.end()) {
            if (statement.if_exists) {
                return {};
            }
            return make_error("no such table: " + statement.table_name);
        }

        if (!write_drop_journal(table_key)) {
            return make_error("failed to write journal");
        }

        std::string error;
        if (!remove_table_files(table_key, error)) {
            clear_journal();
            return make_error(error);
        }

        tables_.erase(it);
        clear_journal();
        return {};
    }

    QueryResult execute_insert(const InsertStatement &statement) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        auto table = find_table_locked(statement.table_name);
        if (!table) {
            return make_error("no such table: " + statement.table_name);
        }

        PreparedInsert prepared;
        prepared.table_name = table->name;
        for (const auto &values : statement.rows) {
            if (values.size() != table->columns.size()) {
                return make_error("column count mismatch");
            }

            Row row;
            row.row_id = table->next_row_id++;
            row.values = values;
            prepared.rows.push_back(std::move(row));
        }

        if (!append_insert_wal(prepared)) {
            return make_error("failed to write insert wal");
        }

        const size_t old_size = table->rows.size();
        table->rows.insert(table->rows.end(), prepared.rows.begin(), prepared.rows.end());
        for (size_t i = old_size; i < table->rows.size(); ++i) {
            index_row(*table, i);
        }

        dirty_tables_.insert(prepared.table_name);
        pending_insert_ops_ += prepared.rows.size();

        std::string checkpoint_error;
        if (pending_insert_ops_ >= kCheckpointInsertOps && !checkpoint_locked(checkpoint_error)) {
            return make_error(checkpoint_error);
        }
        return {};
    }

    QueryResult execute_select(const SelectStatement &statement) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        auto left_table = find_table_locked(statement.from_table);
        if (!left_table) {
            return make_error("no such table: " + statement.from_table);
        }

        std::shared_ptr<Table> right_table;
        if (statement.join.has_value()) {
            right_table = find_table_locked(statement.join->table_name);
            if (!right_table) {
                return make_error("no such table: " + statement.join->table_name);
            }
        }

        QueryResult result;
        if (statement.count_star) {
            long long count = 0;
            iterate_select_rows(*left_table, right_table.get(), statement, [&](const QueryRow &) {
                count++;
            }, result);
            if (!result.is_error) {
                QueryRow row;
                row.column_names = {"COUNT(*)"};
                row.values = {std::to_string(count)};
                result.rows.push_back(std::move(row));
            }
            return result;
        }

        iterate_select_rows(*left_table, right_table.get(), statement, [&](const QueryRow &row) {
            result.rows.push_back(row);
        }, result);
        return result;
    }

    bool open_table_data_fd(Table &table, std::string &error) {
        if (table.data_fd >= 0) {
            return true;
        }
        table.data_fd = ::open(data_path(table.name).c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
        if (table.data_fd < 0) {
            error = "failed to open table data file";
            return false;
        }
        return true;
    }

    bool flush() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        std::string error;
        return checkpoint_locked(error);
    }

    void shutdown() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        std::string ignored;
        checkpoint_locked(ignored);
        for (auto &[_, table] : tables_) {
            if (table && table->data_fd >= 0) {
                ::close(table->data_fd);
                table->data_fd = -1;
            }
        }
        if (wal_fd_ >= 0) {
            ::close(wal_fd_);
            wal_fd_ = -1;
        }
    }

private:
    struct EvalContext {
        const Table *left_table = nullptr;
        const Row *left_row = nullptr;
        const Table *right_table = nullptr;
        const Row *right_row = nullptr;
    };

    QueryResult make_error(const std::string &message) const {
        QueryResult result;
        result.is_error = true;
        result.error = message;
        return result;
    }

    std::shared_ptr<Table> find_table_locked(const std::string &name) const {
        auto it = tables_.find(to_upper(name));
        if (it == tables_.end()) {
            return nullptr;
        }
        return it->second;
    }

    fs::path meta_path(const std::string &table_name) const {
        return fs::path(kDataDir) / (table_name + ".meta");
    }

    fs::path data_path(const std::string &table_name) const {
        return fs::path(kDataDir) / (table_name + ".data");
    }

    bool persist_table_metadata(const Table &table, std::string &error) {
        const int fd = ::open(meta_path(table.name).c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            error = "failed to create metadata file";
            return false;
        }

        std::string content;
        content += encode_token(table.name) + "\n";
        content += std::to_string(table.columns.size()) + "\n";
        for (const auto &column : table.columns) {
            content += encode_token(to_upper(column.name));
            content += " ";
            content += encode_token(column.type);
            content += "\n";
        }

        const bool ok = write_all_fd(fd, content) && sync_file_fd(fd);
        ::close(fd);
        if (!ok) {
            error = "failed to write metadata file";
            return false;
        }
        return fsync_directory(kDataDir);
    }

    bool create_empty_data_file(const std::string &table_name, std::string &error) {
        const int fd = ::open(data_path(table_name).c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            error = "failed to create data file";
            return false;
        }
        const bool ok = sync_file_fd(fd);
        ::close(fd);
        if (!ok) {
            error = "failed to sync data file";
            return false;
        }
        return fsync_directory(kDataDir);
    }

    bool append_rows_to_disk(const std::string &table_name, const std::vector<Row> &rows, std::string &error) {
        auto table = find_table_locked(table_name);
        if (!table) {
            error = "missing table during checkpoint";
            return false;
        }
        if (!open_table_data_fd(*table, error)) {
            error = "failed to open data file";
            return false;
        }

        std::string buffer;
        buffer.reserve(rows.size() * 64);
        for (const auto &row : rows) {
            append_binary_row(buffer, row);
        }
        const bool ok = write_all_fd(table->data_fd, buffer) && sync_file_fd(table->data_fd);
        if (!ok) {
            error = "failed to append data file";
            return false;
        }
        return true;
    }

    bool remove_table_files(const std::string &table_name, std::string &error) {
        auto table = find_table_locked(table_name);
        if (table && table->data_fd >= 0) {
            ::close(table->data_fd);
            table->data_fd = -1;
        }
        std::error_code ec;
        fs::remove(meta_path(table_name), ec);
        if (ec) {
            error = ec.message();
            return false;
        }
        fs::remove(data_path(table_name), ec);
        if (ec) {
            error = ec.message();
            return false;
        }
        return fsync_directory(kDataDir);
    }

    bool write_journal_text(const std::string &content) {
        const int fd = ::open(kJournalPath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            return false;
        }
        const bool ok = write_all_fd(fd, content) && sync_file_fd(fd);
        ::close(fd);
        return ok && fsync_directory(kDataDir);
    }

    bool write_create_journal(const CreateTableStatement &statement) {
        std::string content = "TYPE CREATE\n";
        content += "TABLE " + encode_token(to_upper(statement.table_name)) + "\n";
        content += "COLS " + std::to_string(statement.columns.size()) + "\n";
        for (const auto &column : statement.columns) {
            content += "COL " + encode_token(to_upper(column.name)) + " " + encode_token(column.type) + "\n";
        }
        content += "END\n";
        return write_journal_text(content);
    }

    bool write_drop_journal(const std::string &table_name) {
        std::string content = "TYPE DROP\n";
        content += "TABLE " + encode_token(table_name) + "\n";
        content += "END\n";
        return write_journal_text(content);
    }

    bool write_insert_journal(const PreparedInsert &prepared) {
        std::string content = "TYPE INSERT\n";
        content += "TABLE " + encode_token(prepared.table_name) + "\n";
        content += "ROWS " + std::to_string(prepared.rows.size()) + "\n";
        for (const auto &row : prepared.rows) {
            content += "ROW " + serialize_row(row) + "\n";
        }
        content += "END\n";
        return write_journal_text(content);
    }

    bool append_insert_wal(const PreparedInsert &prepared) {
        if (wal_fd_ < 0) {
            return false;
        }
        std::string buffer;
        buffer.reserve(prepared.rows.size() * 64);
        append_binary_string(buffer, prepared.table_name);
        append_u32(buffer, static_cast<uint32_t>(prepared.rows.size()));
        for (const auto &row : prepared.rows) {
            append_binary_row(buffer, row);
        }
        return write_all_fd(wal_fd_, buffer) && sync_file_fd(wal_fd_);
    }

    bool clear_insert_wal() {
        if (wal_fd_ < 0) {
            return false;
        }
        if (::ftruncate(wal_fd_, 0) != 0) {
            return false;
        }
        if (::lseek(wal_fd_, 0, SEEK_SET) < 0) {
            return false;
        }
        return sync_file_fd(wal_fd_);
    }

    bool ensure_wal_open(std::string &error) {
        if (wal_fd_ >= 0) {
            return true;
        }

        const bool existed = fs::exists(kInsertWalPath);
        wal_fd_ = ::open(kInsertWalPath, O_RDWR | O_CREAT | O_APPEND, 0644);
        if (wal_fd_ < 0) {
            error = "failed to open insert wal";
            return false;
        }
        if (!existed && !fsync_directory(kDataDir)) {
            error = "failed to sync wal directory";
            return false;
        }
        return true;
    }

    void clear_journal() {
        std::error_code ec;
        fs::remove(kJournalPath, ec);
        fsync_directory(kDataDir);
    }

    bool load_tables(std::string &error) {
        tables_.clear();
        for (const auto &entry : fs::directory_iterator(kDataDir)) {
            if (!entry.is_regular_file() || entry.path().extension() != ".meta") {
                continue;
            }

            Table table;
            if (!load_table_from_metadata(entry.path(), table, error) || !load_table_rows(table, error)) {
                return false;
            }
            if (!open_table_data_fd(table, error)) {
                return false;
            }

            auto table_ptr = std::make_shared<Table>(std::move(table));
            tables_[table_ptr->name] = table_ptr;
        }
        return true;
    }

    bool load_table_from_metadata(const fs::path &path, Table &table, std::string &error) {
        std::ifstream input(path);
        if (!input) {
            error = "failed to open metadata file";
            return false;
        }

        std::string line;
        if (!std::getline(input, line)) {
            error = "invalid metadata file";
            return false;
        }
        size_t pos = 0;
        if (!parse_length_prefixed_token(line, pos, table.name)) {
            error = "invalid metadata table name";
            return false;
        }

        if (!std::getline(input, line)) {
            error = "invalid metadata column count";
            return false;
        }
        const size_t column_count = static_cast<size_t>(std::stoull(line));

        table.columns.clear();
        table.column_index.clear();
        for (size_t i = 0; i < column_count; ++i) {
            if (!std::getline(input, line)) {
                error = "invalid metadata column";
                return false;
            }
            pos = 0;
            Column column;
            if (!parse_length_prefixed_token(line, pos, column.name)) {
                error = "invalid metadata column name";
                return false;
            }
            if (pos < line.size() && line[pos] == ' ') {
                pos++;
            }
            if (!parse_length_prefixed_token(line, pos, column.type)) {
                error = "invalid metadata column type";
                return false;
            }
            table.column_index[column.name] = table.columns.size();
            table.columns.push_back(std::move(column));
        }

        table.equality_indexes.clear();
        table.equality_indexes.resize(table.columns.size());
        table.equality_index_ready.assign(table.columns.size(), false);
        table.flushed_rows = 0;
        table.next_row_id = 1;
        return true;
    }

    bool load_table_rows(Table &table, std::string &error) {
        std::ifstream input(data_path(table.name), std::ios::binary);
        if (!input) {
            error = "failed to open data file";
            return false;
        }

        table.rows.clear();
        while (input.peek() != EOF) {
            Row row;
            if (!read_binary_row(input, row)) {
                error = "invalid row data";
                return false;
            }
            table.next_row_id = std::max(table.next_row_id, row.row_id + 1);
            table.rows.push_back(std::move(row));
        }
        rebuild_indexes(table);
        table.flushed_rows = table.rows.size();
        return true;
    }

    bool load_journal(JournalEntry &entry, std::string &error) {
        std::ifstream input(kJournalPath);
        if (!input) {
            error = "failed to open journal";
            return false;
        }

        std::string line;
        while (std::getline(input, line)) {
            if (line == "END") {
                return true;
            }
            if (line.rfind("TYPE ", 0) == 0) {
                entry.type = line.substr(5);
                continue;
            }
            if (line.rfind("TABLE ", 0) == 0) {
                size_t pos = 6;
                if (!parse_length_prefixed_token(line, pos, entry.table_name)) {
                    error = "invalid journal table";
                    return false;
                }
                continue;
            }
            if (line.rfind("COL ", 0) == 0) {
                size_t pos = 4;
                Column column;
                if (!parse_length_prefixed_token(line, pos, column.name)) {
                    error = "invalid journal column name";
                    return false;
                }
                if (pos < line.size() && line[pos] == ' ') {
                    pos++;
                }
                if (!parse_length_prefixed_token(line, pos, column.type)) {
                    error = "invalid journal column type";
                    return false;
                }
                entry.columns.push_back(std::move(column));
                continue;
            }
            if (line.rfind("ROW ", 0) == 0) {
                Row row;
                if (!parse_row_line(line.substr(4), row)) {
                    error = "invalid journal row";
                    return false;
                }
                entry.rows.push_back(std::move(row));
            }
        }

        error = "unterminated journal";
        return false;
    }

    bool load_insert_wal(std::vector<PreparedInsert> &entries, std::string &error) {
        if (!fs::exists(kInsertWalPath)) {
            return true;
        }

        std::ifstream input(kInsertWalPath, std::ios::binary);
        if (!input) {
            error = "failed to open insert wal";
            return false;
        }

        while (input.peek() != EOF) {
            PreparedInsert current;
            uint32_t row_count = 0;
            if (!read_binary_string(input, current.table_name) || !read_u32(input, row_count)) {
                error = "invalid insert wal";
                return false;
            }
            current.rows.reserve(row_count);
            for (uint32_t i = 0; i < row_count; ++i) {
                Row row;
                if (!read_binary_row(input, row)) {
                    error = "invalid insert wal row";
                    return false;
                }
                current.rows.push_back(std::move(row));
            }
            entries.push_back(std::move(current));
        }
        return true;
    }

    bool recover_from_journal(std::string &error) {
        if (!fs::exists(kJournalPath)) {
            return true;
        }

        JournalEntry entry;
        if (!load_journal(entry, error)) {
            return false;
        }

        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (entry.type == "CREATE") {
            if (tables_.count(entry.table_name) == 0) {
                auto table = std::make_shared<Table>();
                table->name = entry.table_name;
                table->columns = entry.columns;
                table->equality_indexes.resize(table->columns.size());
                table->equality_index_ready.assign(table->columns.size(), false);
                table->flushed_rows = 0;
                for (size_t i = 0; i < table->columns.size(); ++i) {
                    table->column_index[to_upper(table->columns[i].name)] = i;
                }
                if (!persist_table_metadata(*table, error) || !create_empty_data_file(table->name, error)) {
                    return false;
                }
                if (!open_table_data_fd(*table, error)) {
                    return false;
                }
                tables_[table->name] = table;
            }
        } else if (entry.type == "DROP") {
            std::string remove_error;
            if (!remove_table_files(entry.table_name, remove_error) && fs::exists(meta_path(entry.table_name))) {
                error = remove_error;
                return false;
            }
            tables_.erase(entry.table_name);
        } else if (entry.type == "INSERT") {
            auto table = find_table_locked(entry.table_name);
            if (!table) {
                error = "journal references missing table";
                return false;
            }

            long long max_existing = table->rows.empty() ? 0 : table->rows.back().row_id;
            std::vector<Row> missing_rows;
            for (const auto &row : entry.rows) {
                if (row.row_id > max_existing) {
                    missing_rows.push_back(row);
                }
            }

            if (!missing_rows.empty()) {
                if (!append_rows_to_disk(entry.table_name, missing_rows, error)) {
                    return false;
                }
                const size_t old_size = table->rows.size();
                table->rows.insert(table->rows.end(), missing_rows.begin(), missing_rows.end());
                for (size_t i = old_size; i < table->rows.size(); ++i) {
                    index_row(*table, i);
                }
                table->flushed_rows = table->rows.size();
                table->next_row_id = std::max(table->next_row_id, missing_rows.back().row_id + 1);
            }
        } else {
            error = "unknown journal entry type";
            return false;
        }

        clear_journal();
        return true;
    }

    bool recover_from_insert_wal(std::string &error) {
        std::vector<PreparedInsert> entries;
        if (!load_insert_wal(entries, error)) {
            return false;
        }
        if (entries.empty()) {
            return true;
        }

        std::unique_lock<std::shared_mutex> lock(mutex_);
        for (const auto &entry : entries) {
            auto table = find_table_locked(entry.table_name);
            if (!table) {
                error = "insert wal references missing table";
                return false;
            }

            long long max_existing = table->rows.empty() ? 0 : table->rows.back().row_id;
            size_t old_size = table->rows.size();
            for (const auto &row : entry.rows) {
                if (row.row_id > max_existing) {
                    table->rows.push_back(row);
                }
            }
            for (size_t i = old_size; i < table->rows.size(); ++i) {
                index_row(*table, i);
            }
            table->next_row_id = std::max(table->next_row_id, table->rows.empty() ? 1LL : table->rows.back().row_id + 1);
            dirty_tables_.insert(table->name);
        }

        if (!checkpoint_locked(error)) {
            return false;
        }
        return true;
    }

    static bool try_parse_number(const std::string &value, long double &out) {
        char *end = nullptr;
        errno = 0;
        out = std::strtold(value.c_str(), &end);
        return errno == 0 && end != nullptr && *end == '\0';
    }

    void rebuild_indexes(Table &table) {
        table.equality_indexes.clear();
        table.equality_indexes.resize(table.columns.size());
        table.equality_index_ready.assign(table.columns.size(), false);
    }

    void index_row(Table &table, size_t row_index) {
        const auto &row = table.rows[row_index];
        for (size_t col = 0; col < row.values.size() && col < table.equality_indexes.size(); ++col) {
            if (table.equality_index_ready[col]) {
                table.equality_indexes[col][row.values[col]].push_back(row_index);
            }
        }
    }

    void ensure_index_built(Table &table, size_t col_idx) const {
        if (col_idx >= table.equality_indexes.size() || table.equality_index_ready[col_idx]) {
            return;
        }
        auto &index = table.equality_indexes[col_idx];
        index.clear();
        for (size_t i = 0; i < table.rows.size(); ++i) {
            index[table.rows[i].values[col_idx]].push_back(i);
        }
        table.equality_index_ready[col_idx] = true;
    }

    std::optional<size_t> resolve_column_index(const Table &table, const std::string &column_name) const {
        auto it = table.column_index.find(to_upper(column_name));
        if (it == table.column_index.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    static std::vector<size_t> all_row_indexes(const Table &table) {
        std::vector<size_t> indexes;
        indexes.reserve(table.rows.size());
        for (size_t i = 0; i < table.rows.size(); ++i) {
            indexes.push_back(i);
        }
        return indexes;
    }

    std::vector<size_t> candidate_rows_for_table(
        Table &table,
        const std::vector<Condition> &conditions,
        const std::string &table_name
    ) const {
        std::vector<size_t> candidates;
        bool initialized = false;

        for (const auto &condition : conditions) {
            if (condition.op != "=") {
                continue;
            }

            const Operand *column_side = nullptr;
            const Operand *literal_side = nullptr;
            if (condition.left.is_column && !condition.right.is_column) {
                column_side = &condition.left;
                literal_side = &condition.right;
            } else if (!condition.left.is_column && condition.right.is_column) {
                column_side = &condition.right;
                literal_side = &condition.left;
            } else {
                continue;
            }

            if (!column_side->column.table.empty() && to_upper(column_side->column.table) != table_name) {
                continue;
            }

            auto col_idx = resolve_column_index(table, column_side->column.column);
            if (!col_idx) {
                continue;
            }

            ensure_index_built(table, *col_idx);
            const auto &index = table.equality_indexes[*col_idx];
            auto it = index.find(literal_side->literal);
            std::vector<size_t> matched = (it == index.end()) ? std::vector<size_t>{} : it->second;
            if (!initialized) {
                candidates = matched;
                initialized = true;
            } else {
                std::vector<size_t> intersection;
                std::set_intersection(
                    candidates.begin(), candidates.end(),
                    matched.begin(), matched.end(),
                    std::back_inserter(intersection));
                candidates = std::move(intersection);
            }
        }

        return initialized ? candidates : all_row_indexes(table);
    }

    std::vector<size_t> candidate_join_rows(
        Table &right_table,
        const Condition &join_condition,
        const EvalContext &ctx
    ) const {
        if (join_condition.op != "=" || !join_condition.left.is_column || !join_condition.right.is_column) {
            return all_row_indexes(right_table);
        }

        ColumnRef probe_column = join_condition.right.column;
        ColumnRef source_column = join_condition.left.column;
        if (!probe_column.table.empty() && to_upper(probe_column.table) != right_table.name) {
            probe_column = join_condition.left.column;
            source_column = join_condition.right.column;
        }

        if (!probe_column.table.empty() && to_upper(probe_column.table) != right_table.name) {
            return all_row_indexes(right_table);
        }

        std::string error;
        auto value = resolve_column_value(ctx, source_column, error);
        if (!value) {
            return {};
        }

        auto col_idx = resolve_column_index(right_table, probe_column.column);
        if (!col_idx) {
            return {};
        }

        ensure_index_built(right_table, *col_idx);
        const auto &index = right_table.equality_indexes[*col_idx];
        auto it = index.find(*value);
        return (it == index.end()) ? std::vector<size_t>{} : it->second;
    }

    bool checkpoint_locked(std::string &error) {
        for (const auto &table_name : dirty_tables_) {
            auto table = find_table_locked(table_name);
            if (!table) {
                continue;
            }
            if (table->flushed_rows >= table->rows.size()) {
                continue;
            }

            std::vector<Row> pending_rows(
                table->rows.begin() + static_cast<std::ptrdiff_t>(table->flushed_rows),
                table->rows.end());
            if (!append_rows_to_disk(table->name, pending_rows, error)) {
                return false;
            }
            table->flushed_rows = table->rows.size();
        }

        dirty_tables_.clear();
        pending_insert_ops_ = 0;
        if (!clear_insert_wal()) {
            error = "failed to clear insert wal";
            return false;
        }
        return true;
    }

    std::optional<std::string> resolve_column_value(const EvalContext &ctx, const ColumnRef &ref, std::string &error) const {
        auto resolve = [&](const Table *table, const Row *row) -> std::optional<std::string> {
            if (!table || !row) {
                return std::nullopt;
            }
            auto it = table->column_index.find(to_upper(ref.column));
            if (it == table->column_index.end()) {
                return std::nullopt;
            }
            return row->values[it->second];
        };

        if (!ref.table.empty()) {
            const std::string table_name = to_upper(ref.table);
            if (ctx.left_table && ctx.left_table->name == table_name) {
                return resolve(ctx.left_table, ctx.left_row);
            }
            if (ctx.right_table && ctx.right_table->name == table_name) {
                return resolve(ctx.right_table, ctx.right_row);
            }
            error = "unknown table qualifier: " + ref.table;
            return std::nullopt;
        }

        auto left_value = resolve(ctx.left_table, ctx.left_row);
        auto right_value = resolve(ctx.right_table, ctx.right_row);
        if (left_value && right_value) {
            error = "ambiguous column: " + ref.column;
            return std::nullopt;
        }
        if (left_value) {
            return left_value;
        }
        if (right_value) {
            return right_value;
        }

        error = "unknown column: " + ref.column;
        return std::nullopt;
    }

    std::optional<std::string> resolve_operand_value(const EvalContext &ctx, const Operand &operand, std::string &error) const {
        if (!operand.is_column) {
            return operand.literal;
        }
        return resolve_column_value(ctx, operand.column, error);
    }

    static bool compare_values(const std::string &lhs, const std::string &rhs, const std::string &op) {
        long double lhs_num = 0;
        long double rhs_num = 0;
        const bool numeric = try_parse_number(lhs, lhs_num) && try_parse_number(rhs, rhs_num);

        if (numeric) {
            if (op == "=") return lhs_num == rhs_num;
            if (op == ">") return lhs_num > rhs_num;
            if (op == "<") return lhs_num < rhs_num;
            if (op == ">=") return lhs_num >= rhs_num;
            if (op == "<=") return lhs_num <= rhs_num;
            return false;
        }

        if (op == "=") return lhs == rhs;
        if (op == ">") return lhs > rhs;
        if (op == "<") return lhs < rhs;
        if (op == ">=") return lhs >= rhs;
        if (op == "<=") return lhs <= rhs;
        return false;
    }

    bool evaluate_condition(const EvalContext &ctx, const Condition &condition, std::string &error) const {
        auto lhs = resolve_operand_value(ctx, condition.left, error);
        if (!lhs) {
            return false;
        }
        auto rhs = resolve_operand_value(ctx, condition.right, error);
        if (!rhs) {
            return false;
        }
        return compare_values(*lhs, *rhs, condition.op);
    }

    QueryRow build_projected_row(const EvalContext &ctx, const SelectStatement &statement, std::string &error) const {
        QueryRow row;
        auto append_all_columns = [&](const Table *table, const Row *data_row) {
            if (!table || !data_row) {
                return;
            }
            for (size_t i = 0; i < table->columns.size(); ++i) {
                row.column_names.push_back(table->columns[i].name);
                row.values.push_back(data_row->values[i]);
            }
        };

        for (const auto &projection : statement.projections) {
            if (projection.is_star) {
                append_all_columns(ctx.left_table, ctx.left_row);
                append_all_columns(ctx.right_table, ctx.right_row);
                continue;
            }

            auto value = resolve_column_value(ctx, projection.column, error);
            if (!value) {
                return {};
            }
            row.column_names.push_back(projection.column.column);
            row.values.push_back(*value);
        }
        return row;
    }

    template <typename Callback>
    void iterate_select_rows(
        Table &left_table,
        Table *right_table,
        const SelectStatement &statement,
        Callback callback,
        QueryResult &result
    ) const {
        auto process_context = [&](const EvalContext &ctx) {
            for (const auto &condition : statement.where_conditions) {
                std::string error;
                if (!evaluate_condition(ctx, condition, error)) {
                    if (!error.empty()) {
                        result = make_error(error);
                    }
                    return;
                }
            }

            if (statement.count_star) {
                callback(QueryRow{});
                return;
            }

            std::string projection_error;
            QueryRow row = build_projected_row(ctx, statement, projection_error);
            if (!projection_error.empty()) {
                result = make_error(projection_error);
                return;
            }
            callback(row);
        };

        if (!right_table) {
            const auto left_candidates = candidate_rows_for_table(left_table, statement.where_conditions, left_table.name);
            for (size_t left_index : left_candidates) {
                const auto &left_row = left_table.rows[left_index];
                process_context(EvalContext{&left_table, &left_row, nullptr, nullptr});
                if (result.is_error) {
                    return;
                }
            }
            return;
        }

        const auto left_candidates = candidate_rows_for_table(left_table, statement.where_conditions, left_table.name);
        for (size_t left_index : left_candidates) {
            const auto &left_row = left_table.rows[left_index];
            EvalContext base_ctx{&left_table, &left_row, right_table, nullptr};
            const auto right_candidates = candidate_join_rows(*right_table, statement.join->on, base_ctx);
            for (size_t right_index : right_candidates) {
                const auto &right_row = right_table->rows[right_index];
                EvalContext ctx{&left_table, &left_row, right_table, &right_row};
                std::string join_error;
                if (!evaluate_condition(ctx, statement.join->on, join_error)) {
                    if (!join_error.empty()) {
                        result = make_error(join_error);
                        return;
                    }
                    continue;
                }

                process_context(ctx);
                if (result.is_error) {
                    return;
                }
            }
        }
    }

    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
    std::unordered_set<std::string> dirty_tables_;
    size_t pending_insert_ops_ = 0;
    int wal_fd_ = -1;
    mutable std::shared_mutex mutex_;
};

Database g_database;

void log_line(const std::string &message) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    std::cout << message << std::endl;
}

bool send_all(int sock, const std::string &payload) {
    size_t sent = 0;
    while (sent < payload.size()) {
        const ssize_t rc = send(sock, payload.data() + sent, payload.size() - sent, kSendFlags);
        if (rc <= 0) {
            return false;
        }
        sent += static_cast<size_t>(rc);
    }
    return true;
}

bool is_statement_terminated(const std::string &buffer, size_t semicolon_pos) {
    bool in_single_quote = false;
    bool in_double_quote = false;

    for (size_t i = 0; i <= semicolon_pos; ++i) {
        const char c = buffer[i];
        const char prev = (i > 0) ? buffer[i - 1] : '\0';

        if (c == '\'' && !in_double_quote && prev != '\\') {
            in_single_quote = !in_single_quote;
        } else if (c == '"' && !in_single_quote && prev != '\\') {
            in_double_quote = !in_double_quote;
        }
    }

    return !in_single_quote && !in_double_quote;
}

bool extract_statement(std::string &pending, std::string &statement) {
    for (size_t i = 0; i < pending.size(); ++i) {
        if (pending[i] == ';' && is_statement_terminated(pending, i)) {
            statement = pending.substr(0, i + 1);
            pending.erase(0, i + 1);
            return true;
        }
    }
    return false;
}

void send_query_result(int client_socket, const QueryResult &result) {
    if (result.is_error) {
        send_all(client_socket, "ERROR: " + result.error + "\nEND\n");
        return;
    }

    std::string response;
    for (const auto &row : result.rows) {
        response += "ROW ";
        response += std::to_string(row.values.size());
        response += " ";
        for (size_t i = 0; i < row.values.size(); ++i) {
            response += encode_token(row.column_names[i]);
            response += encode_token(row.values[i]);
        }
        response += "\n";
    }
    response += "OK\nEND\n";
    send_all(client_socket, response);
}

QueryResult execute_sql(const std::string &sql) {
    const std::string first = first_keyword(sql);
    if (first.empty()) {
        return QueryResult{true, "empty SQL statement", {}};
    }

    if (first == "CREATE") {
        std::string tokenize_error;
        auto tokens = tokenize(sql, tokenize_error);
        if (!tokenize_error.empty() || tokens.empty()) {
            return QueryResult{true, tokenize_error.empty() ? "empty SQL statement" : tokenize_error, {}};
        }
        Parser parser(std::move(tokens));
        std::string parse_error;
        auto statement = parser.parse_create_table(parse_error);
        return statement ? g_database.execute_create_table(*statement) : QueryResult{true, parse_error, {}};
    }
    if (first == "DROP") {
        std::string tokenize_error;
        auto tokens = tokenize(sql, tokenize_error);
        if (!tokenize_error.empty() || tokens.empty()) {
            return QueryResult{true, tokenize_error.empty() ? "empty SQL statement" : tokenize_error, {}};
        }
        Parser parser(std::move(tokens));
        std::string parse_error;
        auto statement = parser.parse_drop_table(parse_error);
        return statement ? g_database.execute_drop_table(*statement) : QueryResult{true, parse_error, {}};
    }
    if (first == "INSERT") {
        InsertStatement fast_statement;
        std::string parse_error;
        if (parse_insert_fast(sql, fast_statement, parse_error)) {
            return g_database.execute_insert(fast_statement);
        }
        std::string tokenize_error;
        auto tokens = tokenize(sql, tokenize_error);
        if (!tokenize_error.empty() || tokens.empty()) {
            return QueryResult{true, tokenize_error.empty() ? parse_error : tokenize_error, {}};
        }
        Parser parser(std::move(tokens));
        auto statement = parser.parse_insert(parse_error);
        return statement ? g_database.execute_insert(*statement) : QueryResult{true, parse_error, {}};
    }
    if (first == "SELECT") {
        std::string tokenize_error;
        auto tokens = tokenize(sql, tokenize_error);
        if (!tokenize_error.empty() || tokens.empty()) {
            return QueryResult{true, tokenize_error.empty() ? "empty SQL statement" : tokenize_error, {}};
        }
        Parser parser(std::move(tokens));
        std::string parse_error;
        auto statement = parser.parse_select(parse_error);
        return statement ? g_database.execute_select(*statement) : QueryResult{true, parse_error, {}};
    }

    return QueryResult{true, "unsupported SQL statement", {}};
}

void handle_client(int client_socket) {
    char buffer[kReadBufferSize];
    std::string pending;

    while (g_running.load()) {
        const ssize_t bytes_read = read(client_socket, buffer, sizeof(buffer));
        if (bytes_read < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (bytes_read == 0) {
            break;
        }

        pending.append(buffer, static_cast<size_t>(bytes_read));

        std::string sql;
        while (extract_statement(pending, sql)) {
            send_query_result(client_socket, execute_sql(sql));
        }
    }

    close(client_socket);
}

void signal_handler(int) {
    g_running.store(false);
}

}  // namespace

int main() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::string error;
    if (!g_database.initialize(error)) {
        std::cerr << "Failed to initialize database: " << error << "\n";
        return 1;
    }

    const int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr("127.0.0.1");
    address.sin_port = htons(kPort);

    if (bind(server_fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) < 0) {
        std::perror("bind");
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, kBacklog) < 0) {
        std::perror("listen");
        close(server_fd);
        return 1;
    }

    log_line("FlexQL Server running on port 9000");

    std::vector<std::thread> workers;
    while (g_running.load()) {
        sockaddr_in client_address {};
        socklen_t client_length = sizeof(client_address);
        const int client_socket = accept(server_fd, reinterpret_cast<sockaddr *>(&client_address), &client_length);
        if (client_socket < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("accept");
            break;
        }

#ifndef MSG_NOSIGNAL
        int one = 1;
        setsockopt(client_socket, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

        workers.emplace_back([client_socket]() {
            handle_client(client_socket);
        });
    }

    close(server_fd);
    for (auto &worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    g_database.shutdown();

    return 0;
}
