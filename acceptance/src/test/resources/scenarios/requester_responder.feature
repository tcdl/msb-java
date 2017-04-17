Feature: Requester-responder test

Scenario: Sends a request to a responder server and waits for response

Given start MSB

And responder server on namespace "test:cucumber"
And requester for namespace "test:cucumber"

When requester sends a request
Then requester gets response in 5000 ms

And shutdown MSB