# chillastic
[![Circle CI](https://circleci.com/gh/groupby/chillastic.svg?style=svg)](https://circleci.com/gh/groupby/chillastic)

Reindex multiple elasticsearch indices, save your progress, mutate your data in-flight.

### REWORKED WITH NEW API
Docs to be updated soon....

### How it works

So what does all this do?

Operations are performed in the following order. None of these steps are mandatory and are only executed if the arguments are provided.

1. Index configurations are run through any relevant mutators and transferred
1. Templates are run through any relevant mutators and transferred
1. Find indices for data transfer based on names provided, then filter and sort those indices.
1. Find all types for each of those indices and filter as needed.
1. A list of pending jobs is created in redis. Each job consists of a index and type, prioritized based on the sorting function provided
1. The requested number of workers are created (1 to # of CPUs)
1. Each worker removes a job from the queue, and then adds it to the completed set once it's been completed with no errors.

If you are forced to stop and restart the process, as long as the completed jobs are left in redis they will not be reprocessed.

### Error Handling
Any errors while transferring the index configurations or templates will halt the process.

If an `es_rejected_execution_exception` is detected during data transfer, those records are retried after a random sleep as this only indicates the target is overwhelmed by input. Any other type of error during data transfer results in the entire job failing and being re-added to the end of the job queue to be tried again later.


