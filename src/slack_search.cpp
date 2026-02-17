#include "slack_search.hpp"
#include "scope_guard.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/logging/logger.hpp"
#include "json_utils.hpp"
#include "slack_client.hpp"
#include "yyjson.hpp"

#include <sstream>
#include <cstring>
#include <algorithm>

namespace duckdb {

namespace {

using namespace duckdb_yyjson; // NOLINT

// Parse JSON response from Slack API
// Slack API returns: {"ok": true, "messages": {"matches": [{"iid": "...", "team": "...", "score": 0, "channel": {"id":
// "...", "name": "..."}, "type": "...", "user": "...", "username": "...", "ts": "...", "text": "...", "permalink":
// "...", "no_reactions": true}, ...]}}
vector<vector<Value>> ParseSlackResponse(const string &json_response) {
	vector<vector<Value>> results;

	yyjson_read_err err;
	auto *doc = yyjson_read_opts((char *)json_response.c_str(), json_response.size(), 0, nullptr, &err); // NOLINT
	if (!doc) {
		throw IOException("Failed to parse Slack response JSON at byte %lld: %s", err.pos, err.msg);
	}
	SCOPE_EXIT {
		yyjson_doc_free(doc);
	};

	auto *root = yyjson_doc_get_root(doc);
	if (!root || !yyjson_is_obj(root)) {
		return results;
	}

	auto *messages = yyjson_obj_get(root, "messages");
	if (!messages || !yyjson_is_obj(messages)) {
		return results;
	}

	auto *matches = yyjson_obj_get(messages, "matches");
	if (!matches || !yyjson_is_arr(matches)) {
		return results;
	}

	size_t idx, max;
	yyjson_val *match;
	yyjson_arr_foreach(matches, idx, max, match) {
		if (results.size() >= 10) {
			break;
		}
		if (!match || !yyjson_is_obj(match)) {
			continue;
		}

		// Extract relevant fields only
		auto iid = JsonUtils::GetStringField(match, "iid");
		auto username = JsonUtils::GetStringField(match, "username");
		auto ts_str = JsonUtils::GetStringField(match, "ts");
		auto text = JsonUtils::GetStringField(match, "text");
		auto permalink = JsonUtils::GetStringField(match, "permalink");

		// Extract channel name
		string channel_name;
		auto *channel_obj = yyjson_obj_get(match, "channel");
		if (channel_obj && yyjson_is_obj(channel_obj)) {
			channel_name = JsonUtils::GetStringField(channel_obj, "name");
		}

		// Convert Slack timestamp (Unix timestamp with microseconds) to DuckDB timestamp
		// Slack ts format: "1771304649.527509" (seconds.microseconds)
		Value timestamp_value;
		if (!ts_str.empty()) {
			try {
				const double ts_double = std::stod(ts_str);
				const int64_t micros = static_cast<int64_t>(ts_double * 1000000);
				timestamp_value = Value::TIMESTAMP(timestamp_t(micros));
			} catch (...) {
				// If parsing fails, use NULL timestamp
				timestamp_value = Value(LogicalType {LogicalTypeId::TIMESTAMP});
			}
		} else {
			timestamp_value = Value(LogicalType {LogicalTypeId::TIMESTAMP});
		}

		vector<Value> row;
		row.reserve(6);
		row.emplace_back(Value(iid));
		row.emplace_back(Value(channel_name));
		row.emplace_back(Value(username));
		row.emplace_back(timestamp_value);
		row.emplace_back(Value(text));
		row.emplace_back(Value(permalink));

		results.emplace_back(row);
	}

	return results;
}

// Call Slack search API using the HTTP client
vector<vector<Value>> SearchSlack(ExecutionContext &context, const string &query) {
	string json_response = SlackClient::SearchMessagesRaw(query);
	DUCKDB_LOG_DEBUG(context, "Slack API HTTP response: %s", json_response.c_str());
	return ParseSlackResponse(json_response);
}

unique_ptr<FunctionData> SlackSearchBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.empty() || input.inputs.size() > 1) {
		throw BinderException("search_slack expects exactly one argument (search query)");
	}

	if (input.inputs[0].IsNull()) {
		throw BinderException("search_slack query cannot be NULL");
	}

	string query = input.inputs[0].GetValue<string>();

	return_types.push_back(LogicalType {LogicalTypeId::VARCHAR}); // iid
	names.push_back("iid");

	return_types.push_back(LogicalType {LogicalTypeId::VARCHAR}); // channel_name
	names.push_back("channel");

	return_types.push_back(LogicalType {LogicalTypeId::VARCHAR}); // username
	names.push_back("username");

	return_types.push_back(LogicalType {LogicalTypeId::TIMESTAMP}); // timestamp (converted from Slack ts)
	names.push_back("timestamp");

	return_types.push_back(LogicalType {LogicalTypeId::VARCHAR}); // text
	names.push_back("text");

	return_types.push_back(LogicalType {LogicalTypeId::VARCHAR}); // permalink
	names.push_back("permalink");

	return make_uniq<SlackSearchBindData>(query);
}

unique_ptr<LocalTableFunctionState> SlackSearchLocalInit(ExecutionContext &context, TableFunctionInitInput &input,
                                                         GlobalTableFunctionState *global_state) {
	auto &bind_data = input.bind_data->Cast<SlackSearchBindData>();
	auto local_state = make_uniq<SlackSearchLocalState>();

	// Perform the search
	try {
		local_state->results = SearchSlack(context, bind_data.search_query);
		local_state->initialized = true;
	} catch (const std::exception &e) {
		throw IOException("Failed to search Slack: %s", e.what());
	}

	return std::move(local_state);
}

void SlackSearchFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.local_state->Cast<SlackSearchLocalState>();

	if (!state.initialized || state.current_index >= state.results.size()) {
		output.SetCardinality(0);
		return;
	}

	idx_t output_count = 0;
	idx_t max_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.results.size() - state.current_index);

	for (idx_t i = 0; i < max_count; i++) {
		if (state.current_index + i >= state.results.size()) {
			break;
		}

		auto &row = state.results[state.current_index + i];
		for (idx_t col = 0; col < output.ColumnCount() && col < row.size(); col++) {
			output.data[col].SetValue(output_count, row[col]);
		}
		output_count++;
	}

	state.current_index += output_count;
	output.SetCardinality(output_count);
}

} // namespace

void RegisterSlackSearchFunction(ExtensionLoader &loader) {
	TableFunction search_slack_function("search_slack", {LogicalType {LogicalTypeId::VARCHAR}}, SlackSearchFunction,
	                                    SlackSearchBind, nullptr, SlackSearchLocalInit);
	loader.RegisterFunction(search_slack_function);
}

} // namespace duckdb
