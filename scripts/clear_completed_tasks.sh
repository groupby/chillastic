#! /bin/bash

completed_key="${1:-"100.00"}"
for task in $(curl -s chillastic.groupbycloud.com/tasks | jq 'to_entries[] | select(.value.percentComplete == "'"${completed_key}"'") | .key'); do
  id="$(echo ${task} | cut -c2-34)"
  curl -s "chillastic.groupbycloud.com/tasks/${id}" -XDELETE | jq .
done
