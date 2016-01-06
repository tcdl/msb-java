Lifecycle:
Before:
Given init MSB context contextResponder
And init MSB context contextRequester
After:
Outcome: ANY
Then shutdown context contextRequester
And shutdown context contextResponder

Scenario: Sends a request to a responder server and gets response
Given responder server from contextResponder listens on namespace test:jbehave
And responder server responds with '{"result": "hello jbehave"}'
And requester from contextRequester sends requests to namespace test:jbehave
When requester from contextRequester sends a request
Then requester gets response in 5000 ms
And response equals
|result|
|hello jbehave|

When init MSB context contextResponder
And responder server from contextResponder listens on namespace test:jbehave
And responder server responds with '{"result": "hello jbehave"}'
And requester from contextRequester sends a request
Then requester gets response in 5000 ms
And response equals
|result|
|hello jbehave|
