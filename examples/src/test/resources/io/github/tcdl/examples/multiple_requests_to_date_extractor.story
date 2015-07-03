Lifecycle:
Before:
Given init
And microservice DateExtractor
After:
Outcome: ANY
Given shutdown

Scenario: Sending multiple requests to date extractor microservice in parallel

Given 10 requesters send a request to namespace search:parsers:facets:v1 with query 'Holidays in 2015'
Then wait responses in 2000 ms