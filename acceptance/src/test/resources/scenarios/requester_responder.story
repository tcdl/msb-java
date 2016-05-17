Lifecycle:
Before:
Given MSB configuration with consumer prefetch count 20
And start MSB
And clear log
After:
Outcome: ANY
Then shutdown MSB
Then reset mock responses

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

Scenario: Sends a request to a responder server with a custom tag

Given responder server responds with '{"result": "hello jbehave"}'
And responder server listens on namespace test:jbehave
And requester sends requests to namespace test:jbehave
When requester sends a request with tag 'customTagKey:MY_CUSTOM_TAG'
Then requester gets response in 5000 ms
And responder requests received count equals 1
And response equals
|result|
|hello jbehave|
And log contains 'tags[customTagKey:MY_CUSTOM_TAG]'
And log contains 'customTagKey[MY_CUSTOM_TAG]'

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

Scenario: Requester processes callback with a delay

Given responder server responds with '{"result": "hello jbehave"}'
And responder server listens on namespace test:jbehave
And responder will provide 10 responses
And requester (with 1000 ms request timeout to receive 10 responses) sends requests to namespace test:jbehave
And requester will process responses with 200 ms delay
When requester sends a request
Then requester will get all responses in 5000 ms
And responder requests received count equals 1


Scenario: Message forwarding

Given responder server responds with '{"result": "hello jbehave - forwarding"}'
And responder server listens on namespace test:jbehave
And requester sets forwarding to test:jbehave:forwarding and sends requests to namespace test:jbehave
When requester sends a request
Then requester gets response in 5000 ms
And responder requests received count equals 1
And request forward namespace equals test:jbehave:forwarding

Scenario: Requester sends request for single future response

Given responder server responds with '{"result": "hello jbehave - future"}'
And responder server listens on namespace test:jbehave
When requester sends a request for single result to namespace test:jbehave
And requester blocks waiting for response for 5000 ms
Then resolved response equals
|result|
|hello jbehave - future|

Scenario: Actual response comes after acknowledge

Given next response from responder contains acknowledge with 1 remaining response
And next response from responder contains body '{"result": "hello jbehave - future"}'
And responder server responds sequentially on namespace test:jbehave
When requester sends a request for single result to namespace test:jbehave
And requester blocks waiting for response for 5000 ms
Then resolved response equals
|result|
|hello jbehave - future|

Scenario: To many responses

Given next response from responder contains acknowledge with 2 remaining response
And next response from responder contains body '{"result": "hello jbehave - future"}'
And responder server responds sequentially on namespace test:jbehave
When requester sends a request for single result to namespace test:jbehave
Then requester gets exception when tries to obtain result
