#include "slack_client.hpp"

#include "duckdb/common/exception.hpp"
#include "json_utils.hpp"
#include "yyjson.hpp"

#include <curl/curl.h>
#include <cstring>

namespace duckdb {

namespace {

using namespace duckdb_yyjson; // NOLINT

// Callback function to write HTTP response data
size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
	((string *)userp)->append((char *)contents, size * nmemb);
	return size * nmemb;
}

} // namespace

string SlackClient::ExtractApiError(const string &json_response) {
	yyjson_read_err err;
	auto *doc = yyjson_read_opts((char *)json_response.c_str(), json_response.size(), 0, nullptr, &err); // NOLINT
	if (!doc) {
		return "";
	}

	auto *root = yyjson_doc_get_root(doc);
	auto error = JsonUtils::GetStringField(root, "error");
	yyjson_doc_free(doc);
	return error;
}

string SlackClient::SearchMessagesRaw(const string &query) {
	// Get Slack token from environment variable
	const char *token = getenv("SLACK_API_TOKEN");
	if (!token || strlen(token) == 0) {
		throw InvalidInputException("SLACK_API_TOKEN environment variable is not set. Please set it before using search_slack.");
	}
	
	// Initialize curl
	CURL *curl = curl_easy_init();
	if (!curl) {
		throw InternalException("Failed to initialize CURL");
	}
	
	// Build URL with query parameter
	char *encoded_query = curl_easy_escape(curl, query.c_str(), query.length());
	if (!encoded_query) {
		curl_easy_cleanup(curl);
		throw InternalException("Failed to URL encode query");
	}
	
	string url = "https://slack.com/api/search.messages?query=";
	url += encoded_query;
	url += "&count=10";
	
	curl_free(encoded_query);
	
	// Set authorization header
	struct curl_slist *headers = nullptr;
	string auth_header = "Authorization: Bearer ";
	auth_header += token;
	headers = curl_slist_append(headers, auth_header.c_str());
	headers = curl_slist_append(headers, "Content-Type: application/json");
	
	string readBuffer;
	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	
	CURLcode res = curl_easy_perform(curl);
	
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
		string error_msg = ExtractApiError(readBuffer);
		if (!error_msg.empty()) {
			throw IOException("Slack API error: %s", error_msg.c_str());
		}
		throw IOException("Slack API returned an error. Response: %s", readBuffer.c_str());
	}
	
	return readBuffer;
}

} // namespace duckdb
