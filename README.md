MSB-Java [![Project status](https://travis-ci.org/tcdl/msb-java.svg?branch=master)](https://travis-ci.org/tcdl/msb-java) [![Coverage Status](http://img.shields.io/coveralls/tcdl/msb-java/master.svg)](https://coveralls.io/r/tcdl/msb-java?branch=master)
===========

Microservice bus - Java API

Required tools:
---------------
* JDK (8 or higher)
* Maven (version 3), main build tool

Bintray support release configuration:
--------------------------------------
If you're part of the tcdl bintray organization (https://bintray.com/tcdl) and have sufficient rights you can publish releases to bintray (https://bintray.com/tcdl/releases/msb-java/view).

For this you'll need to add a `server` to the `servers` section of your settings.xml:
```
<server>
  <id>bintray-msb-java</id>
  <username>[YOUR_USERNAME]</username>
  <password>[YOUR_API_TOKEN]</password>
</server>
```

Bintray / jcenter SNAPSHOT publishing configuration:
----------------------------------------
If you're part of the tcdl bintray organization (https://bintray.com/tcdl) and have sufficient rights you can publish snapshots to jfrog / jcenter (http://oss.jfrog.org/artifactory/simple/oss-snapshot-local/io/github/tcdl/).

For this you'll need to add a `server` to the `servers` section of your settings.xml:
```
<server>
  <id>oss-jfrog-msb-java</id>
  <username>[YOUR_BINTRAY_USERNAME]</username>
  <password>[YOUR_BINTRAY_API_TOKEN]</password>
</server>
```

##API

Class: MsbContext <a name="msb-context"/>
---------------------------------------------------
Specifies the context of the MSB message processing.
This is environment where microservice will be run, it holds all necessary information such as
bus configuration, service details, schema for incoming and outgoing messages, factory for building requests
and responses etc.
 
#### new MsbContext.MsbContextBuilder.build()
Create context and initialize it with configuration from reference.conf(property file inside MSB library)
or application.conf, which will override library properties.

Class: RequestOptions <a name="request-options"/>
---------------------------------------------------
Options used while constructing Requester that specify number and time to wait for acknowledgements or responses.

- **ackTimeout** period of time in milliseconds to wait for acknowledge to increase timeout or number of responses to expect
- **responseTimeout** period of time in milliseconds before ending request
- **waitForResponses** number of expected responses before ending or timing out

Class: Requester
---------------------------------------------------
Enable user send message to bus and process responses for this messages if any expected.

#### create(namespace, requestOptions, [originalMessage], msbContext)
Factory method to create a new instance of a requester

- **namespace** topic name to listen on for requests
- **requestOptions** specify number and time to wait for acknowledgements or responses [options](#request-options)
- **originalMessage** original message (to take correlation id from)
- **msbContext** MSB application [context](#msb-context)

#### publish(request)
Publish a request to the message bus

- **request** payload  

#### onAcknowledge(Callback handler) 
Assign a handler on acknowledge message 

#### onResponse(Callback handler) 
Assign a handler on response message
 
#### onEnd(Callback handler) 
Assign a handler on event when all responses were received or timeout happened

Class: ResponderServer
---------------------------------------------------
Component for creating microservices which will listen for incoming requests, execute business logic
and respond.

#### create(namespace, messageTemplate, msbContext, requestHandler)
Factory method to create a new instance of a responder server

- **namespace** topic name to listen on for requests
- **messageTemplate** template which is used to construct a request/response message
- **msbContext** MSB application [context](#msb-context)
- **requestHandler** handler for processing the request

#### listen()
start listening for incoming requests

Interface: Responder
---------------------------------------------------
Responsible for creating responses and acknowledgements and sending them to the bus.

#### sendAck([timeoutMs], [responsesRemaining])
Send acknowledge message

- **timeoutMs** period of time a requester should wait until ending 
- **responsesRemaining** number of responses(payload messages) a requester should wait for from this responder

#### sendPayload(response)
Send payload message

- **response** payload to be sent in response to incoming request 


