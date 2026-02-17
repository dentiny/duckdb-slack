#define DUCKDB_EXTENSION_MAIN

#include "quack_extension.hpp"

#include "slack_search.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

namespace {
void LoadInternal(ExtensionLoader &loader) {
	// Register Slack search table function
	RegisterSlackSearchFunction(loader);
}
} // namespace

void QuackExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string QuackExtension::Name() {
	return "quack";
}

std::string QuackExtension::Version() const {
#ifdef EXT_VERSION_QUACK
	return EXT_VERSION_QUACK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(quack, loader) {
	duckdb::LoadInternal(loader);
}
}
