#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "flexql.h"

using namespace std;
using namespace std::chrono;

namespace {

struct WorkloadResult {
    atomic<long long> successful_ops{0};
    atomic<long long> failed_ops{0};
    atomic<long long> total_latency_us{0};
    atomic<bool> printed_error{false};
};

void record_error_once(WorkloadResult &result, const string &workload, const string &message) {
    bool expected = false;
    if (result.printed_error.compare_exchange_strong(expected, true)) {
        cout << "[" << workload << "] first error: " << message << "\n";
    }
}

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

bool exec_count_query(FlexQL *db, const string &sql, long long &count, string *error = nullptr) {
    struct State {
        long long count = 0;
        bool seen = false;
    } state;

    auto callback = [](void *arg, int argc, char **argv, char **) -> int {
        State *state = static_cast<State *>(arg);
        if (argc > 0 && argv && argv[0]) {
            state->count = atoll(argv[0]);
            state->seen = true;
        }
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

    count = state.seen ? state.count : 0;
    return true;
}

bool reset_workload_tables(FlexQL *db) {
    return exec_sql(db, "DROP TABLE IF EXISTS PERF_USERS;") &&
           exec_sql(db, "DROP TABLE IF EXISTS PERF_EVENTS;") &&
           exec_sql(db, "CREATE TABLE PERF_USERS(ID DECIMAL, NAME VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);") &&
           exec_sql(db, "CREATE TABLE PERF_EVENTS(ID DECIMAL, USER_ID DECIMAL, AMOUNT DECIMAL, EXPIRES_AT DECIMAL);");
}

bool seed_workload_tables(FlexQL *db, int rows, int batch_size) {
    int inserted = 0;
    while (inserted < rows) {
        stringstream users_sql;
        stringstream events_sql;
        users_sql << "INSERT INTO PERF_USERS VALUES ";
        events_sql << "INSERT INTO PERF_EVENTS VALUES ";

        int batch_rows = 0;
        while (batch_rows < batch_size && inserted < rows) {
            const int id = inserted + 1;
            users_sql << "(" << id << ", 'user" << id << "', " << (1000 + (id % 5000)) << ", 1893456000)";
            events_sql << "(" << id << ", " << id << ", " << (10 + (id % 250)) << ", 1893456000)";
            inserted++;
            batch_rows++;
            if (batch_rows < batch_size && inserted < rows) {
                users_sql << ",";
                events_sql << ",";
            }
        }
        users_sql << ";";
        events_sql << ";";

        if (!exec_sql(db, users_sql.str()) || !exec_sql(db, events_sql.str())) {
            return false;
        }
    }
    return true;
}

void print_summary(const string &name, const WorkloadResult &result, milliseconds elapsed) {
    const long long ok = result.successful_ops.load();
    const long long failed = result.failed_ops.load();
    const long long total = ok + failed;
    const long long avg_latency_us = ok > 0 ? result.total_latency_us.load() / ok : 0;
    const long long throughput = elapsed.count() > 0 ? (ok * 1000LL) / elapsed.count() : ok;

    cout << "\n[" << name << "]\n";
    cout << "Elapsed: " << elapsed.count() << " ms\n";
    cout << "Successful ops: " << ok << "\n";
    cout << "Failed ops: " << failed << "\n";
    cout << "Total ops: " << total << "\n";
    cout << "Throughput: " << throughput << " ops/sec\n";
    cout << "Average successful op latency: " << avg_latency_us << " us\n";
}

bool run_read_heavy(const char *host, int port, int thread_count, int operations_per_thread) {
    WorkloadResult result;
    vector<thread> threads;
    const auto start = steady_clock::now();

    for (int t = 0; t < thread_count; ++t) {
        threads.emplace_back([=, &result]() {
            FlexQL *db = nullptr;
            if (flexql_open(host, port, &db) != FLEXQL_OK) {
                result.failed_ops.fetch_add(operations_per_thread);
                record_error_once(result, "Read-heavy workload", "flexql_open failed");
                return;
            }

            mt19937 rng(1000 + t);
            uniform_int_distribution<int> user_dist(1, 5000);
            for (int i = 0; i < operations_per_thread; ++i) {
                const int user_id = user_dist(rng);
                stringstream sql;
                sql << "SELECT COUNT(*) "
                       "FROM PERF_USERS INNER JOIN PERF_EVENTS ON PERF_USERS.ID = PERF_EVENTS.USER_ID "
                       "WHERE PERF_USERS.ID = " << user_id << " AND PERF_EVENTS.AMOUNT >= 10;";

                const auto op_start = steady_clock::now();
                string error;
                long long rows = 0;
                if (exec_count_query(db, sql.str(), rows, &error)) {
                    const auto op_end = steady_clock::now();
                    result.successful_ops.fetch_add(1);
                    result.total_latency_us.fetch_add(duration_cast<microseconds>(op_end - op_start).count());
                } else {
                    result.failed_ops.fetch_add(1);
                    record_error_once(result, "Read-heavy workload", error);
                }
            }

            flexql_close(db);
        });
    }

    for (thread &th : threads) {
        th.join();
    }

    print_summary("Read-heavy workload", result, duration_cast<milliseconds>(steady_clock::now() - start));
    return result.failed_ops.load() == 0;
}

bool run_write_heavy(const char *host, int port, int thread_count, int operations_per_thread, int batch_size) {
    WorkloadResult result;
    vector<thread> threads;
    const auto start = steady_clock::now();

    for (int t = 0; t < thread_count; ++t) {
        threads.emplace_back([=, &result]() {
            FlexQL *db = nullptr;
            if (flexql_open(host, port, &db) != FLEXQL_OK) {
                result.failed_ops.fetch_add(operations_per_thread);
                record_error_once(result, "Write-heavy workload", "flexql_open failed");
                return;
            }

            for (int op = 0; op < operations_per_thread; ) {
                stringstream sql;
                sql << "INSERT INTO PERF_EVENTS VALUES ";

                int rows = 0;
                while (rows < batch_size && op < operations_per_thread) {
                    const long long id = 1000000LL + (static_cast<long long>(t) * operations_per_thread) + op + 1;
                    const int user_id = (op % 5000) + 1;
                    const int amount = 25 + ((op + t) % 300);
                    sql << "(" << id << ", " << user_id << ", " << amount << ", 1893456000)";
                    rows++;
                    op++;
                    if (rows < batch_size && op < operations_per_thread) {
                        sql << ",";
                    }
                }
                sql << ";";

                const auto op_start = steady_clock::now();
                string error;
                if (exec_sql(db, sql.str(), &error)) {
                    const auto op_end = steady_clock::now();
                    result.successful_ops.fetch_add(rows);
                    result.total_latency_us.fetch_add(duration_cast<microseconds>(op_end - op_start).count());
                } else {
                    result.failed_ops.fetch_add(rows);
                    record_error_once(result, "Write-heavy workload", error);
                }
            }

            flexql_close(db);
        });
    }

    for (thread &th : threads) {
        th.join();
    }

    print_summary("Write-heavy workload", result, duration_cast<milliseconds>(steady_clock::now() - start));
    return result.failed_ops.load() == 0;
}

bool run_mixed_multi_client(const char *host, int port, int thread_count, int operations_per_thread) {
    WorkloadResult result;
    vector<thread> threads;
    const auto start = steady_clock::now();

    for (int t = 0; t < thread_count; ++t) {
        threads.emplace_back([=, &result]() {
            FlexQL *db = nullptr;
            if (flexql_open(host, port, &db) != FLEXQL_OK) {
                result.failed_ops.fetch_add(operations_per_thread);
                record_error_once(result, "Mixed multi-client workload", "flexql_open failed");
                return;
            }

            mt19937 rng(2000 + t);
            uniform_int_distribution<int> coin(0, 1);
            uniform_int_distribution<int> user_dist(1, 5000);

            for (int i = 0; i < operations_per_thread; ++i) {
                stringstream sql;
                if (coin(rng) == 0) {
                    const int user_id = user_dist(rng);
                    sql << "SELECT NAME, BALANCE FROM PERF_USERS WHERE ID = " << user_id << ";";
                } else {
                    const long long id = 2000000LL + (static_cast<long long>(t) * operations_per_thread) + i + 1;
                    const int user_id = user_dist(rng);
                    sql << "INSERT INTO PERF_EVENTS VALUES (" << id << ", " << user_id << ", "
                        << (50 + (i % 400)) << ", 1893456000);";
                }

                const auto op_start = steady_clock::now();
                string error;
                if (exec_sql(db, sql.str(), &error)) {
                    const auto op_end = steady_clock::now();
                    result.successful_ops.fetch_add(1);
                    result.total_latency_us.fetch_add(duration_cast<microseconds>(op_end - op_start).count());
                } else {
                    result.failed_ops.fetch_add(1);
                    record_error_once(result, "Mixed multi-client workload", error);
                }
            }

            flexql_close(db);
        });
    }

    for (thread &th : threads) {
        th.join();
    }

    print_summary("Mixed multi-client workload", result, duration_cast<milliseconds>(steady_clock::now() - start));
    return result.failed_ops.load() == 0;
}

}  // namespace

int main(int argc, char **argv) {
    const char *host = "127.0.0.1";
    const int port = 9000;
    const int seed_rows = (argc > 1) ? max(1000, atoi(argv[1])) : 5000;
    const int thread_count = (argc > 2) ? max(1, atoi(argv[2])) : 4;
    const int operations_per_thread = (argc > 3) ? max(1, atoi(argv[3])) : 1000;
    const int write_batch_size = (argc > 4) ? max(1, atoi(argv[4])) : 25;

    FlexQL *setup_db = nullptr;
    if (flexql_open(host, port, &setup_db) != FLEXQL_OK) {
        cerr << "Cannot open FlexQL\n";
        return 1;
    }

    cout << "Preparing workload tables...\n";
    if (!reset_workload_tables(setup_db)) {
        cerr << "Failed to reset workload tables\n";
        flexql_close(setup_db);
        return 1;
    }
    if (!seed_workload_tables(setup_db, seed_rows, write_batch_size)) {
        cerr << "Failed to seed workload tables\n";
        flexql_close(setup_db);
        return 1;
    }
    flexql_close(setup_db);

    cout << "Seed rows: " << seed_rows << "\n";
    cout << "Threads: " << thread_count << "\n";
    cout << "Operations per thread: " << operations_per_thread << "\n";
    cout << "Write batch size: " << write_batch_size << "\n";

    bool ok = true;
    ok = run_read_heavy(host, port, thread_count, operations_per_thread) && ok;
    ok = run_write_heavy(host, port, thread_count, operations_per_thread, write_batch_size) && ok;
    ok = run_mixed_multi_client(host, port, thread_count, operations_per_thread) && ok;

    cout << "\nOverall result: " << (ok ? "PASS" : "FAIL") << "\n";
    return ok ? 0 : 1;
}
