# DuckDB Slack Extension

A DuckDB extension that enables searching Slack messages directly from SQL queries.

## What is this extension?

The DuckDB Slack extension provides a table function `search_slack()` that allows you to query Slack messages using DuckDB's SQL interface. It integrates with Slack's search API to return message results that can be analyzed, filtered, and joined with other data sources in DuckDB.

## How to use it

### Setup

1. **Get a Slack API Token**

   Create a Slack app with the `search:read` scope, or use an existing User OAuth Token (`xoxp-`). The token must have permission to search messages.

2. **Set the Environment Variable**

   ```sh
   export SLACK_API_TOKEN="xoxp-your-token-here"
   ```

### Usage

#### Installation

```sql
D FORCE INSTALL duckdb_slack FROM community;
D INSTALL duckdb_slack;
```

#### Basic Search

Search for messages containing specific keywords:

```sql
SELECT *
FROM search_slack('incident status')
LIMIT 10;
```

#### Available Columns

The `search_slack()` function returns the following columns:

- `iid` - Unique message identifier
- `channel` - Channel name where the message was posted
- `username` - Username of the message author
- `timestamp` - Message timestamp (DuckDB TIMESTAMP type)
- `text` - Message text content
- `permalink` - Direct link to the message in Slack

### Notes

- The extension searches across all channels accessible to your Slack token
- Results are limited to the top 10 matches by default (as per Slack API)
- The search uses Slack's native search algorithm, which performs word-boundary matching
- Timestamps are automatically converted to DuckDB's TIMESTAMP type for easy date/time operations
