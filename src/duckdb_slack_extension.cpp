#define DUCKDB_EXTENSION_MAIN

#include "duckdb_slack_extension.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "slack_search.hpp"

namespace duckdb {

namespace {
void LoadInternal(ExtensionLoader &loader) {
	// Register Slack search table function
	RegisterSlackSearchFunction(loader);
}
} // namespace

void DuckdbSlackExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string DuckdbSlackExtension::Name() {
	return "duckdb_slack";
}

std::string DuckdbSlackExtension::Version() const {
#ifdef EXT_VERSION_DUCKDB_SLACK
	return EXT_VERSION_DUCKDB_SLACK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(duckdb_slack, loader) {
	duckdb::LoadInternal(loader);
}
}
