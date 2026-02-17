#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Slack API client for making HTTP requests
class SlackClient {
public:
	// Search Slack messages using the search.messages API
	// Returns the raw JSON response as a string
	static string SearchMessagesRaw(const string &query);

	// Extract error message from Slack API error response
	static string ExtractApiError(const string &json_response);
};

} // namespace duckdb
