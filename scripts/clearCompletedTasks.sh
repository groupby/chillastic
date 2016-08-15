#! /bin/bash

for task in $(curl -s chillastic.groupbycloud.com/tasks | jq 'to_entries[] | select(.value.percentComplete == "100.00") | .key'); do
  id="$(echo ${task} | cut -c2-34)"
  curl -s "chillastic.groupbycloud.com/tasks/${id}" -XDELETE | jq .
done
