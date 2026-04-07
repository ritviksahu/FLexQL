#include <chrono>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "flexql.h"

using namespace std;
using namespace std::chrono;

namespace {

struct QueryStats {
    long long queries = 0;
    long long rows_seen = 0;
    long long elapsed_ms = 0;
};

bool exec_sql(FlexQL *db, const string &sql, string *error = nullptr) {
    char *err = nullptr;
    const int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &err);
    if (rc == FLEXQL_OK) {
        return true;
    }
    if (error) {
        *error = err ? err : "unknown error";
    }
    if (err) {
        flexql_free(err);
    }
    return false;
}

bool run_counting_query(FlexQL *db, const string &sql, long long &rows_seen, string *error = nullptr) {
    struct State {
        long long rows = 0;
    } state;

    auto callback = [](void *arg, int, char **, char **) -> int {
        State *state = static_cast<State *>(arg);
        state->rows++;
        return 0;
    };

    char *err = nullptr;
    const int rc = flexql_exec(db, sql.c_str(), callback, &state, &err);
    if (rc != FLEXQL_OK) {
        if (error) {
            *error = err ? err : "unknown error";
        }
        if (err) {
            flexql_free(err);
        }
        return false;
    }

    rows_seen = state.rows;
    return true;
}

bool reset_tables(FlexQL *db) {
    return exec_sql(db, "DROP TABLE IF EXISTS SEL_USERS;") &&
           exec_sql(db, "DROP TABLE IF EXISTS SEL_EVENTS;") &&
           exec_sql(db, "CREATE TABLE SEL_USERS(ID DECIMAL, NAME VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);") &&
           exec_sql(db, "CREATE TABLE SEL_EVENTS(ID DECIMAL, USER_ID DECIMAL, AMOUNT DECIMAL, EXPIRES_AT DECIMAL);");
}

bool seed_tables(FlexQL *db, int rows, int batch_size) {
    int inserted = 0;
    while (inserted < rows) {
        stringstream user_sql;
        stringstream event_sql;
        user_sql << "INSERT INTO SEL_USERS VALUES ";
        event_sql << "INSERT INTO SEL_EVENTS VALUES ";

        int batch_rows = 0;
        while (batch_rows < batch_size && inserted < rows) {
            const int id = inserted + 1;
            user_sql << "(" << id << ", 'user" << id << "', " << (1000 + (id % 5000)) << ", 1893456000)";
            event_sql << "(" << id << ", " << id << ", " << (10 + (id % 250)) << ", 1893456000)";
            inserted++;
            batch_rows++;
            if (batch_rows < batch_size && inserted < rows) {
                user_sql << ",";
                event_sql << ",";
            }
        }

        user_sql << ";";
        event_sql << ";";
        if (!exec_sql(db, user_sql.str()) || !exec_sql(db, event_sql.str())) {
            return false;
        }
    }
    return true;
}

QueryStats run_query_set(FlexQL *db, const string &label, const vector<string> &queries, int rounds) {
    QueryStats stats;
    const auto start = steady_clock::now();

    for (int round = 0; round < rounds; ++round) {
        for (const auto &query : queries) {
            long long rows = 0;
            string error;
            if (!run_counting_query(db, query, rows, &error)) {
                cerr << "[" << label << "] failed: " << error << "\n";
                return stats;
            }
            stats.queries++;
            stats.rows_seen += rows;
        }
    }

    stats.elapsed_ms = duration_cast<milliseconds>(steady_clock::now() - start).count();
    return stats;
}

void print_stats(const string &label, const QueryStats &stats) {
    const long long qps = stats.elapsed_ms > 0 ? (stats.queries * 1000LL) / stats.elapsed_ms : stats.queries;
    const long long avg_rows = stats.queries > 0 ? stats.rows_seen / stats.queries : 0;

    cout << "\n[" << label << "]\n";
    cout << "Queries: " << stats.queries << "\n";
    cout << "Elapsed: " << stats.elapsed_ms << " ms\n";
    cout << "Throughput: " << qps << " queries/sec\n";
    cout << "Average rows returned: " << avg_rows << "\n";
}

vector<string> make_point_selects(int seed_rows, int query_count) {
    vector<string> queries;
    queries.reserve(query_count);
    for (int i = 0; i < query_count; ++i) {
        const int id = (i % seed_rows) + 1;
        stringstream sql;
        sql << "SELECT NAME, BALANCE FROM SEL_USERS WHERE ID = " << id << ";";
        queries.push_back(sql.str());
    }
    return queries;
}

vector<string> make_range_selects(int seed_rows, int query_count) {
    vector<string> queries;
    queries.reserve(query_count);
    for (int i = 0; i < query_count; ++i) {
        const int low = ((i * 97) % max(1, seed_rows - 200)) + 1;
        const int high = low + 199;
        stringstream sql;
        sql << "SELECT ID, NAME FROM SEL_USERS WHERE ID >= " << low << " AND ID <= " << high << ";";
        queries.push_back(sql.str());
    }
    return queries;
}

vector<string> make_join_selects(int seed_rows, int query_count) {
    vector<string> queries;
    queries.reserve(query_count);
    for (int i = 0; i < query_count; ++i) {
        const int id = (i % seed_rows) + 1;
        stringstream sql;
        sql << "SELECT SEL_USERS.NAME, SEL_EVENTS.AMOUNT "
               "FROM SEL_USERS INNER JOIN SEL_EVENTS ON SEL_USERS.ID = SEL_EVENTS.USER_ID "
               "WHERE SEL_USERS.ID = " << id << " AND SEL_EVENTS.AMOUNT >= 10;";
        queries.push_back(sql.str());
    }
    return queries;
}

vector<string> make_count_queries(int seed_rows, int query_count) {
    vector<string> queries;
    queries.reserve(query_count);
    for (int i = 0; i < query_count; ++i) {
        const int low = ((i * 53) % max(1, seed_rows - 500)) + 1;
        const int high = low + 499;
        stringstream sql;
        sql << "SELECT COUNT(*) FROM SEL_USERS WHERE ID >= " << low << " AND ID <= " << high << ";";
        queries.push_back(sql.str());
    }
    return queries;
}

}  // namespace

int main(int argc, char **argv) {
    const char *host = "127.0.0.1";
    const int port = 9000;
    const int seed_rows = (argc > 1) ? max(1000, atoi(argv[1])) : 100000;
    const int query_count = (argc > 2) ? max(10, atoi(argv[2])) : 1000;
    const int rounds = (argc > 3) ? max(1, atoi(argv[3])) : 5;
    const int insert_batch_size = (argc > 4) ? max(1, atoi(argv[4])) : 1000;

    FlexQL *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        cerr << "Cannot open FlexQL\n";
        return 1;
    }

    cout << "Preparing select benchmark tables...\n";
    if (!reset_tables(db)) {
        cerr << "Failed to reset select benchmark tables\n";
        flexql_close(db);
        return 1;
    }
    if (!seed_tables(db, seed_rows, insert_batch_size)) {
        cerr << "Failed to seed select benchmark tables\n";
        flexql_close(db);
        return 1;
    }

    cout << "Seed rows: " << seed_rows << "\n";
    cout << "Queries per set: " << query_count << "\n";
    cout << "Rounds: " << rounds << "\n";
    cout << "Insert batch size: " << insert_batch_size << "\n";

    const auto point_queries = make_point_selects(seed_rows, query_count);
    const auto range_queries = make_range_selects(seed_rows, query_count);
    const auto join_queries = make_join_selects(seed_rows, query_count);
    const auto count_queries = make_count_queries(seed_rows, query_count);

    const QueryStats point_stats = run_query_set(db, "Point SELECT benchmark", point_queries, rounds);
    const QueryStats range_stats = run_query_set(db, "Range SELECT benchmark", range_queries, rounds);
    const QueryStats join_stats = run_query_set(db, "JOIN SELECT benchmark", join_queries, rounds);
    const QueryStats count_stats = run_query_set(db, "COUNT benchmark", count_queries, rounds);

    print_stats("Point SELECT benchmark", point_stats);
    print_stats("Range SELECT benchmark", range_stats);
    print_stats("JOIN SELECT benchmark", join_stats);
    print_stats("COUNT benchmark", count_stats);

    flexql_close(db);
    return 0;
}
