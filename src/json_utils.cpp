#include "json_utils.hpp"

namespace duckdb {

string JsonUtils::GetStringField(yyjson_val *obj, const char *field) {
	if (!obj || !yyjson_is_obj(obj)) {
		return "";
	}

	auto *value = yyjson_obj_get(obj, field);
	if (!value || !yyjson_is_str(value)) {
		return "";
	}

	auto *str = yyjson_get_str(value);
	if (!str) {
		return "";
	}
	return string(str, yyjson_get_len(value));
}

double JsonUtils::GetNumberField(yyjson_val *obj, const char *field) {
	if (!obj || !yyjson_is_obj(obj)) {
		return 0.0;
	}

	auto *value = yyjson_obj_get(obj, field);
	if (!value || !yyjson_is_num(value)) {
		return 0.0;
	}

	return yyjson_get_num(value);
}

bool JsonUtils::GetBoolField(yyjson_val *obj, const char *field) {
	if (!obj || !yyjson_is_obj(obj)) {
		return false;
	}

	auto *value = yyjson_obj_get(obj, field);
	if (!value || !yyjson_is_bool(value)) {
		return false;
	}

	return yyjson_get_bool(value);
}

} // namespace duckdb
