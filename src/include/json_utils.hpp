#pragma once

#include "duckdb.hpp"
#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson; // NOLINT

// JSON utility functions for extracting values from yyjson objects
class JsonUtils {
public:
	// Extract a string field from a JSON object
	static string GetStringField(yyjson_val *obj, const char *field);

	// Extract a number field from a JSON object
	static double GetNumberField(yyjson_val *obj, const char *field);

	// Extract a boolean field from a JSON object
	static bool GetBoolField(yyjson_val *obj, const char *field);
};

} // namespace duckdb
