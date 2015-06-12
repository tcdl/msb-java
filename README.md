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
MSB application context which contains configuration and other beans required for MSB
 
#### new MsbContext.MsbContextBuilder.build()
Creates a default context

Class: MsbMessageOptions <a name="message-options"/>
---------------------------------------------------
Represents message options to override default configuration

- **namespace** topic name to listen on for requests
- **ackTimeout** period of time in milliseconds to wait for acknowledge to increase timeout or number of responses to expect
- **responseTimeout** period of time in milliseconds before ending request
- **waitForResponses** number of expected responses before ending or timing out

Class: Requester
---------------------------------------------------
A component which can send requests into the message bus and listen for responses

#### create(messageOptions, [originalMessage], msbContext)
Factory method to create a new instance of a requester

- **messageOptions** message [options](#message-options)  
- **msbContext** MSB application [context](#msb-context)

#### publish(request)
Publish a request to the message bus

- **request** payload  

#### onAcknowledge(Callback handler) 
Assign a handler on acknowledge message 

#### onResponse(Callback handler) 
Assign a handler on response message
 
#### onEnd(Callback handler) 
Assign a handler on event when all responses were received 

#### onError(Callback handler) 
Assign a handler on error 

Class: ResponderServer
---------------------------------------------------
A component which listens for incoming requests and can send responses

#### create(messageOptions, msbContext)
Factory method to create a new instance of a responder

- **messageOptions** message [options](#message-options)
- **msbContext** MSB application [context](#msb-context)

#### use(middleware)
Submit a handler or a list of handlers

- **middleware** callback or array of request handlers with 

signature `handler(request, responder)`

- **request** payload of the incoming message
- **responder** [responder](#responder) object

#### listen()
start listening for incoming requests


Class: Responder <a name="responder"/>
---------------------------------------------------
Sends acknowledge and response messages

#### new Responder(messageOptions, originalMessage, msbContext) 
Creates a new instance of a responder

- **messageOptions** message [options](#message-options) 
- **msbContext** MSB application [context](#msb-context)

#### sendAck([timeoutMs], [responsesRemaining])
Send acknowledge message

- **timeoutMs** period of time a requester should wait until ending 
- **responsesRemaining** number of responses(payload messages) a requester should wait for from this responder

#### sendPayload(response)
Send payload message

- **response** payload to be sent in response to incoming request 


