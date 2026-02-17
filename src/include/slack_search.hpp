#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct SlackSearchBindData : public TableFunctionData {
	explicit SlackSearchBindData(const string &query) : search_query(query) {
	}

	string search_query;
};

struct SlackSearchLocalState : public LocalTableFunctionState {
	SlackSearchLocalState() : current_index(0), initialized(false) {
	}

	idx_t current_index;
	bool initialized;
	vector<vector<Value>> results;
};

void RegisterSlackSearchFunction(ExtensionLoader &loader);

} // namespace duckdb
