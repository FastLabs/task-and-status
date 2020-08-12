#Task and Status 

An event driven hierarchical task executor

Examples:
- An event type triggers a particular job
- There are task hierarchies that waits to multiple events to happen in order to trigger a particular job - to test this

##TODO:

Some validation rules may be necesary to validate before we route a task-action
1. validate the hierarchy match: 
2. validate if the matched nodes contain the requested event type; 
3. validate if all mandatory task attributes are available, from root task and from matched tasks


1. Append task to existing hierarchy - think this is the first priority - implemented
2. Validate the hierarchy 
3. apply orchestration rules  - implemented

1. Start thinking on how we pas task context information from one event to another
2. Concurrent running of the jobs to be tested, it should be handled