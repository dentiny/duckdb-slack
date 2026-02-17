#include "slack_search.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"
#include <curl/curl.h>
#include <sstream>
#include <cstring>
#include <algorithm>

namespace duckdb {

namespace {

// Helper function to replace all occurrences of a substring
void ReplaceAll(string &str, const string &from, const string &to) {
	if (from.empty()) {
		return;
	}
	size_t start_pos = 0;
	while ((start_pos = str.find(from, start_pos)) != string::npos) {
		str.replace(start_pos, from.length(), to);
		start_pos += to.length();
	}
}

// Callback function to write HTTP response data
size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
	((std::string *)userp)->append((char *)contents, size * nmemb);
	return size * nmemb;
}

// Helper to extract JSON string value
string ExtractJsonString(const string &json, const string &key) {
	string search_key = "\"" + key + "\":";
	size_t key_pos = json.find(search_key);
	if (key_pos == string::npos) {
		return "";
	}
	
	// Find the value after the colon
	size_t value_start = json.find_first_not_of(" \t\n\r", key_pos + search_key.length());
	if (value_start == string::npos) {
		return "";
	}
	
	// Check if it's a string value
	if (json[value_start] != '"') {
		// Not a string, might be a number or object
		return "";
	}
	
	value_start++; // Skip opening quote
	size_t value_end = value_start;
	bool escape_next = false;
	
	while (value_end < json.length()) {
		if (escape_next) {
			escape_next = false;
			value_end++;
			continue;
		}
		if (json[value_end] == '\\') {
			escape_next = true;
			value_end++;
			continue;
		}
		if (json[value_end] == '"') {
			break;
		}
		value_end++;
	}
	
	if (value_end >= json.length()) {
		return "";
	}
	
	string result = json.substr(value_start, value_end - value_start);
	// Unescape basic JSON strings
	ReplaceAll(result, "\\n", "\n");
	ReplaceAll(result, "\\t", "\t");
	ReplaceAll(result, "\\\"", "\"");
	ReplaceAll(result, "\\\\", "\\");
	ReplaceAll(result, "\\/", "/");
	
	return result;
}

// Parse JSON response from Slack API
// Slack API returns: {"ok": true, "messages": {"matches": [{"text": "...", "channel": {"name": "..."}, "user": "...", "ts": "...", "permalink": "..."}, ...]}}
vector<vector<Value>> ParseSlackResponse(const string &json_response) {
	vector<vector<Value>> results;
	
	// Check if response is OK
	if (json_response.find("\"ok\":false") != string::npos) {
		// API returned an error
		return results;
	}
	
	// Find matches array - look for "matches":[
	size_t matches_pos = json_response.find("\"matches\"");
	if (matches_pos == string::npos) {
		return results;
	}
	
	// Find the opening bracket of the matches array
	size_t array_start = json_response.find("[", matches_pos);
	if (array_start == string::npos) {
		return results;
	}
	
	// Parse each match object in the array
	size_t pos = array_start + 1;
	int depth = 0;
	bool in_string = false;
	bool escape_next = false;
	size_t obj_start = 0;
	
	while (pos < json_response.length() && results.size() < 10) {
		char c = json_response[pos];
		
		if (escape_next) {
			escape_next = false;
			pos++;
			continue;
		}
		
		if (c == '\\') {
			escape_next = true;
			pos++;
			continue;
		}
		
		if (c == '"' && !escape_next) {
			in_string = !in_string;
		} else if (!in_string) {
			if (c == '{') {
				if (depth == 0) {
					obj_start = pos;
				}
				depth++;
			} else if (c == '}') {
				depth--;
				if (depth == 0) {
					// Found a complete match object
					string match_obj = json_response.substr(obj_start, pos - obj_start + 1);
					
					// Extract fields
					string text = ExtractJsonString(match_obj, "text");
					
					// Extract channel name (nested in channel object)
					string channel = "";
					size_t channel_obj_start = match_obj.find("\"channel\":{");
					if (channel_obj_start != string::npos) {
						size_t channel_obj_end = match_obj.find("}", channel_obj_start);
						if (channel_obj_end != string::npos) {
							string channel_obj = match_obj.substr(channel_obj_start, channel_obj_end - channel_obj_start + 1);
							channel = ExtractJsonString(channel_obj, "name");
						}
					}
					
					string user = ExtractJsonString(match_obj, "user");
					string timestamp = ExtractJsonString(match_obj, "ts");
					string permalink = ExtractJsonString(match_obj, "permalink");
					
					vector<Value> row;
					row.push_back(Value(text));
					row.push_back(Value(channel));
					row.push_back(Value(user));
					row.push_back(Value(timestamp));
					row.push_back(Value(permalink));
					results.push_back(row);
				}
			} else if (c == ']' && depth == 0) {
				// End of matches array
				break;
			}
		}
		pos++;
	}
	
	return results;
}

// Call Slack search API
vector<vector<Value>> SearchSlack(const string &query) {
	vector<vector<Value>> results;
	
	// Get Slack token from environment variable
	const char *token = getenv("SLACK_API_TOKEN");
	if (!token || strlen(token) == 0) {
		throw InvalidInputException("SLACK_API_TOKEN environment variable is not set. Please set it before using search_slack.");
	}
	
	// Initialize curl (global initialization is not needed in modern curl)
	CURL *curl;
	CURLcode res;
	std::string readBuffer;
	
	curl = curl_easy_init();
	if (!curl) {
		throw InternalException("Failed to initialize CURL");
	}
	
	// Build URL with query parameter
	// Use curl's URL encoding
	char *encoded_query = curl_easy_escape(curl, query.c_str(), query.length());
	if (!encoded_query) {
		curl_easy_cleanup(curl);
		throw InternalException("Failed to URL encode query");
	}
	
	std::string url = "https://slack.com/api/search.messages?query=";
	url += encoded_query;
	url += "&count=10";
	
	curl_free(encoded_query);
	
	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
	
	// Set authorization header
	struct curl_slist *headers = nullptr;
	std::string auth_header = "Authorization: Bearer ";
	auth_header += token;
	headers = curl_slist_append(headers, auth_header.c_str());
	headers = curl_slist_append(headers, "Content-Type: application/json");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	
	res = curl_easy_perform(curl);
	
	if (res != CURLE_OK) {
		curl_slist_free_all(headers);
		curl_easy_cleanup(curl);
		throw IOException("CURL error: %s", curl_easy_strerror(res));
	}
	
	long response_code;
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
	
	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);
	
	if (response_code != 200) {
		throw IOException("Slack API returned error code: %ld. Response: %s", response_code, readBuffer.c_str());
	}
	
	// Check if API returned an error in the JSON response
	if (readBuffer.find("\"ok\":false") != string::npos) {
		// Try to extract error message
		string error_msg = ExtractJsonString(readBuffer, "error");
		if (!error_msg.empty()) {
			throw IOException("Slack API error: %s", error_msg.c_str());
		}
		throw IOException("Slack API returned an error. Response: %s", readBuffer.c_str());
	}
	
	// Parse response
	results = ParseSlackResponse(readBuffer);
	
	return results;
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
	
	// Define return types and column names
	return_types.push_back(LogicalType::VARCHAR); // text
	names.push_back("text");
	
	return_types.push_back(LogicalType::VARCHAR); // channel
	names.push_back("channel");
	
	return_types.push_back(LogicalType::VARCHAR); // user
	names.push_back("user");
	
	return_types.push_back(LogicalType::VARCHAR); // timestamp
	names.push_back("timestamp");
	
	return_types.push_back(LogicalType::VARCHAR); // permalink
	names.push_back("permalink");
	
	return make_uniq<SlackSearchBindData>(query);
}

unique_ptr<LocalTableFunctionState> SlackSearchLocalInit(ExecutionContext &context,
                                                                TableFunctionInitInput &input,
                                                                GlobalTableFunctionState *global_state) {
	auto &bind_data = input.bind_data->Cast<SlackSearchBindData>();
	auto local_state = make_uniq<SlackSearchLocalState>();
	
	// Perform the search
	try {
		local_state->results = SearchSlack(bind_data.search_query);
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
	TableFunction search_slack_function("search_slack", {LogicalType::VARCHAR}, SlackSearchFunction,
	                                    SlackSearchBind, nullptr, SlackSearchLocalInit);
	loader.RegisterFunction(search_slack_function);
}

} // namespace duckdb
