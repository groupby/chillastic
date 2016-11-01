#!/usr/bin/env bash
set -e

curl localhost:8080/mutators/someNamespace/indexMutator -H 'Content-type: text/plain' --data-binary '@examples/mutators/indexDate.js'
curl localhost:8080/filters/someNamespace/indexFilter -H 'Content-type: text/plain' --data-binary '@examples/filters/indices.js'
curl -XPOST localhost:8080/tasks/example -d '{"source":{"host":"localhost","port":9200},"destination":{"host":"localhost","port":9201},"transfer":{"documents":{"fromIndices":"*","filters":{"actions":[{"namespace":"someNamespace","id":"indexFilter"}]}}},"mutators":{"actions":[{"namespace":"someNamespace","id":"indexMutator"}]}}'