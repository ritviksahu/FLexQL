#include <iostream>
#include <string>
#include <vector>

#include "flexql.h"

using namespace std;

namespace {

bool looks_complete_statement(const string &sql) {
    bool in_string = false;
    for (size_t i = 0; i < sql.size(); ++i) {
        if (sql[i] == '\'') {
            if (in_string && i + 1 < sql.size() && sql[i + 1] == '\'') {
                i++;
                continue;
            }
            in_string = !in_string;
        }
    }
    return !in_string && !sql.empty() && sql.back() == ';';
}

int print_callback(void *, int argc, char **argv, char **col_names) {
    for (int i = 0; i < argc; ++i) {
        if (i != 0) {
            cout << " | ";
        }
        cout << (col_names && col_names[i] ? col_names[i] : "") << "="
             << (argv && argv[i] ? argv[i] : "NULL");
    }
    cout << "\n";
    return 0;
}

}  // namespace

int main(int argc, char **argv) {
    const char *host = (argc > 1) ? argv[1] : "127.0.0.1";
    const int port = (argc > 2) ? atoi(argv[2]) : 9000;

    FlexQL *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        cerr << "Cannot open FlexQL at " << host << ":" << port << "\n";
        return 1;
    }

    cout << "FlexQL REPL connected to " << host << ":" << port << "\n";
    cout << "Type SQL ending with ';'. Type 'exit' or 'quit' to leave.\n";

    while (true) {
        cout << "flexql> " << flush;

        string line;
        if (!getline(cin, line)) {
            cout << "\n";
            break;
        }

        if (line == "exit" || line == "quit") {
            break;
        }

        string sql = line;
        while (!looks_complete_statement(sql)) {
            cout << "......> " << flush;
            if (!getline(cin, line)) {
                cout << "\n";
                flexql_close(db);
                return 0;
            }
            sql += "\n";
            sql += line;
        }

        char *err = nullptr;
        const int rc = flexql_exec(db, sql.c_str(), print_callback, nullptr, &err);
        if (rc != FLEXQL_OK) {
            cerr << "ERROR: " << (err ? err : "unknown error") << "\n";
        } else {
            cout << "OK\n";
        }
        if (err) {
            flexql_free(err);
        }
    }

    flexql_close(db);
    return 0;
}
