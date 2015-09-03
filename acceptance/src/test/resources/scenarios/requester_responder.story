Lifecycle:
Before:
Given start MSB
And responder server listens on namespace test:jbehave
After:
Outcome: ANY
Then shutdown MSB

Scenario: Sends a request to a responder server and waits for response

Given responder server responds with '{"result": "hello jbehave"}'
And requester sends requests to namespace test:jbehave
When requester sends a request
Then requester gets response in 2000 ms
And response equals
|result|
|hello jbehave|


