Lifecycle:
Before:
Given start MSB
After:
Outcome: ANY
Then shutdown MSB

Scenario: Sends a request to a responder server and waits for response

Given responder server responds with '{"result": "hello jbehave"}'
And responder server listens on namespace test:jbehave
And requester sends requests to namespace test:jbehave
When requester sends a request
Then requester gets response in 5000 ms
And responder requests received count equals 1
And response equals
|result|
|hello jbehave|

Scenario: Responder confirms a message manually

Given responder server responds with '{"result": "hello jbehave - manual confirm"}'
And responder server listens on namespace test:jbehave
And responder server will confirm next request
And requester sends requests to namespace test:jbehave
When requester sends a request
Then requester gets response in 5000 ms
And responder requests received count equals 1
And response equals
|result|
|hello jbehave - manual confirm|

Scenario: Responder rejects a first message delivery so it will be redelivered

Given responder server responds with '{"result": "hello jbehave - manual reject"}'
And responder server listens on namespace test:jbehave
And responder server will reject next request
And requester sends requests to namespace test:jbehave
When requester sends a request
Then requester gets response in 5000 ms
And responder requests received count equals 2
And response equals
|result|
|hello jbehave - manual reject|


