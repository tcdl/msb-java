Lifecycle:
Before:
Given init
And microservice DateExtractor
After:
Outcome: ANY
Given shutdown

Scenario: Parsing date with date extractor microservice

Given requester sends requests to namespace search:parsers:facets:v1
When requester sends a request with query 'Holidays in 2015'
Then requester gets response in 2000 ms
And response contains
|results|
|year=15|

When requester sends a request with query '2015-counter-2078'
Then requester gets response in 2000 ms
And response contains
|results|
|year=15|
