Lifecycle:
Before:
Given MSB configuration with consumer thread pool size 1
And start MSB
And clear log
After:
Outcome: ANY
Then shutdown MSB

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