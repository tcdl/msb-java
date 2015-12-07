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


Scenario: Responder ask to retry a first message delivery so it will be redelivered

Given responder server responds with '{"result": "hello jbehave - manual retry"}'
And responder server listens on namespace test:jbehave
And responder server will confirm all requests
And responder server will retry next request
And requester sends requests to namespace test:jbehave
When requester sends a request
Then requester gets response in 5000 ms
And responder requests received count equals 2
And response equals
|result|
|hello jbehave - manual retry|


Scenario: Responder confirms all incoming messages manually

Given responder server responds with '{"result": "hello jbehave - manual confirm"}'
And responder server listens on namespace test:jbehave
And responder server will confirm all requests
And requester sends requests to namespace test:jbehave
When requester sends a request
Then requester gets response in 5000 ms
And responder requests received count equals 1
And response equals
|result|
|hello jbehave - manual confirm|


Scenario: Responder retrys all incoming messages manually

Given responder server responds with '{"result": "hello jbehave - constant retry"}'
And responder server listens on namespace test:jbehave
And responder server will retry all requests
And requester sends requests to namespace test:jbehave
When requester sends a request
Then requester does not get a response in 1000 ms
And responder requests received count equals 2


Scenario: Responder rejctes all incoming messages manually

Given responder server responds with '{"result": "hello jbehave - constant reject"}'
And responder server listens on namespace test:jbehave
And responder server will reject all requests
And requester sends requests to namespace test:jbehave
When requester sends a request
Then requester does not get a response in 1000 ms
And responder requests received count equals 1


Scenario: Responder confirms all incoming messages manually in a different thread

Given responder server responds with '{"result": "hello jbehave - manual confirm"}'
And responder server listens on namespace test:jbehave
And responder server will confirm all requests
And responder server will send acknowledge and response from a new thread
And requester sends requests to namespace test:jbehave
When requester sends a request
Then requester gets response in 5000 ms
And responder requests received count equals 1
And response equals
|result|
|hello jbehave - manual confirm|


Scenario: Responder retrys all incoming messages manually in a different thread

Given responder server responds with '{"result": "hello jbehave - constant retry"}'
And responder server listens on namespace test:jbehave
And responder server will retry all requests
And responder server will send acknowledge and response from a new thread
And requester sends requests to namespace test:jbehave
When requester sends a request
Then requester does not get a response in 1000 ms
And responder requests received count equals 2


