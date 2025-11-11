#!/bin/sh

echo "Example: Printing git status as Arrow IPC stream"
./target/release/git-status2arrow-ipc-stream | arrow-cat

echo
echo example 2 using sql
./target/release/git-status2arrow-ipc-stream |
	rs-ipc-stream2df \
	--max-rows 1024 \
	--tabname 'git_status' \
	--sql "
		SELECT
			path,
			status,
			item_type,
			extension,
			size,
			last_modification_time
		FROM git_status
		WHERE
			status IN ('Modified', 'Added')
			AND extension IN ('rs', 'toml', 'md', 'sh')
		ORDER BY path
	" |
	rs-arrow-ipc-stream-cat
