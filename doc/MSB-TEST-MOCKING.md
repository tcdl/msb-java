# MSB-Java mocking support
This documents describes experimental MSB-Java mocking approaches.
There are two different testing approaches available: AdapterFactory-based and ObjectFactory-based.
Both of them are Mockito-based.

## Maven dependency
In order to use mocking utilities described, please use the following Maven dependency:
```
    <dependency>
        <groupId>io.github.tcdl.msb</groupId>
        <artifactId>msb-java-core</artifactId>
        <type>test-jar</type>
        <version>${msb.version}</version>
        <scope>test</scope>
    </dependency>
```

## ObjectFactory-based approach
**Related package:** _io.github.tcdl.msb.mock.objectfactory_

This type of test support is used to test client's code. For example, it gives an ability
to invoke incoming handlers directly (with a custom test payload as an argument).
So MSB internal code is not involved in the testing.

Typical usage:
 - Inject mocked MsbContext configured to return TestMsbObjectFactory and extract a storage from this mock:

``` java
@Mock
private MsbContext msbContext;
private TestMsbStorageForObjectFactory storage;
...
@Before
public void setUp() throws Exception {
    when(msbContext.getObjectFactory())
            .thenReturn(new TestMsbObjectFactory());
    storage = TestMsbStorageForObjectFactory.extract(msbContext);
    ...
}
```

 - Perform captured requester testing (including direct handlers invocations):
``` java
RequesterCapture<MyPalyload> requesterCapture =
        storage.getRequesterCapture("my:namespace:out");
assertEquals(MyPalyload.class, requesterCapture.getPayloadClass());

//invoke onEnd handler
Callback<Void> onEndHandler = requesterCapture.getOnEndCaptor().getValue();
onEndHandler.call(null);

//invoke onResponse handler
BiConsumer<MyPalyload, MessageContext> onResponseHandler = requesterCapture.getOnResponseCaptor().getValue();
onResponseHandler.accept(myPalyload, messageContextMock);
```

 - Perform captured responder testing (including direct handlers invocations):
``` java
ResponderCapture<MyPalyload> responderCapture = storage.getResponderCapture("my:namespace:in");
assertEquals(MyPalyload.class, responderCapture.getPayloadClass());

//invoke requestHandler
ResponderServer.RequestHandler<MyPalyload> requestHandler = responderCapture.getRequestHandler();
requestHandler.process(myPalyload, responderContextMock);
```

## AdapterFactory-based approach
**Related package:** _io.github.tcdl.msb.mock.adapterfactory_

This type of test support is used to test the flow of an incoming message through all MSB layers.
Using this option makes it possible both submit a raw message JSON into a namespace, and capture outgoing messages raw JSON.

Typical usage:
 - Configure Msb to use the test AdapterFactory implementation:
```
msbConfig {
    brokerAdapterFactory = "io.github.tcdl.msb.mock.adapterfactory.TestMsbAdapterFactory"
}
```

 - Inject non-mocked MsbContext that is using this configuration and extract a storage from this mock:
``` java
private MsbContext msbContext;
private TestMsbStorageForAdapterFactory storage;
...
@Before
public void setUp() throws Exception {
    msbContext = TestUtils.createSimpleMsbContext();
    storage = TestMsbStorageForAdapterFactory.extract(msbContext);
}

```

 - Submit incoming messages JSON using:
``` java
storage.publishIncomingMessage("my:incoming:namespace", "{my: 'message_json'}");
```

 - Verify outgoing message JSON using:
``` java
List<String> publishedTestMessages = storage.getOutgoingMessages("my:out:namespace");
String publishedMessage = storage.getOutgoingMessage("my:out:namespace");
```

 - During the testing, perform JSON String - Message conversions using MSB utilities if required:
``` java
Utils.fromJson(json, Message.class, objectMapper);
Utils.toJson(message, objectMapper);
```

 - If several MsbContext instances are involved into the testing, they will be able to handle only their own messages.
 In order to use shared messaging, it is required to connect MsbContext instances to a single storage:
``` java
MsbContext otherContext = TestUtils.createSimpleMsbContext();
storage.connect(otherContext);
```
