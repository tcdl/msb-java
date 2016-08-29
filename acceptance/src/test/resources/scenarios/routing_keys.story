Given 1st responder server listens on namespace test:namespace with routing keys routing-key-1, routing-key-2
Given 2nd responder server listens on namespace test:namespace with routing keys routing-key-3
When requester sends to test:namespace a request with body '{"messageId":"rk1"}' and routing key routing-key-1
When requester sends to test:namespace a request with body '{"messageId":"rk2"}' and routing key routing-key-2
When requester sends to test:namespace a request with body '{"messageId":"rk3"}' and routing key routing-key-3
Then 1st responder receives only messages '{"messageId":"rk1"}', '{"messageId":"rk2"}'
Then 2nd responder receives only messages '{"messageId":"rk3"}'