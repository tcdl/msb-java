Lifecycle:
Before:
Given start MSB
After:
Outcome: ANY
Then shutdown MSB

Scenario: consumers receive messages according to routing keys
Given 1st responder server listens on namespace test:namespace with routing keys routing-key-1, routing-key-2
Given 2nd responder server listens on namespace test:namespace with routing keys routing-key-3
When requester sends to test:namespace a request with body '{"messageId":"rk1"}' and routing key routing-key-1
When requester sends to test:namespace a request with body '{"messageId":"rk2"}' and routing key routing-key-2
When requester sends to test:namespace a request with body '{"messageId":"rk3"}' and routing key routing-key-3
Then 1st responder receives only messages '{"messageId":"rk1"}', '{"messageId":"rk2"}'
Then 2nd responder receives only messages '{"messageId":"rk3"}'

Scenario: routing key is ignored for messages that require forwarding
Given responder server listens on fanout namespace test:routing
And requester sets forwarding to test:routing:forwarding and target namespace to test:routing
When requester sends to test:routing a request with forward namespace test:routing:forwarding, body '{"messageId":"rk1"}' and routing key routing-key-1
Then message envelope topics section is
|to          |forward                |response|routingKey   |
|test:routing|test:routing:forwarding|null    |routing-key-1|
When requester sends to test:routing a request with forward namespace test:routing:forwarding, body '{"messageId":"rk2"}' without routing key
Then message envelope topics section is
|to          |forward                |response|routingKey|
|test:routing|test:routing:forwarding|null    |          |