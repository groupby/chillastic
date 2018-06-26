#!/bin/bash

completed_key="${1:-"100.00"}"
for task in $(acurl -s chillastic.groupbycloud.com/tasks | jq 'to_entries[] | select(.value.percentComplete == "'${completed_key}'") | .key '); do
  id="$(echo ${task} | cut -c2-34)"
  acurl -s "chillastic.groupbycloud.com/tasks/${id}" -XDELETE | jq .
done
