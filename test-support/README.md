# MSB-Java Test support
This experimental module provides a test support library for MSB.
There are two different testing approaches available: AdapterFactory-based and ObjectFactory-based.
The library is based on Mockito.

## ObjectFactory-based approach
**Related package:** _io.github.tcdl.msb.testsupport.objectfactory_

This type of test support is used to test client's code. For example, it gives an ability
to invoke incoming handlers directly (with a custom test payload as an argument).
So MSB internal code is not involved in the testing.

Typical usage:
 - Inject mocked MsbContext configured to return TestMsbObjectFactory:
 ```
    @Mock
    private MsbContext msbContext;
    ...
    @Before
    public void setUp() throws Exception {
        when(msbContext.getObjectFactory())
                .thenReturn(new TestMsbObjectFactory());
        ...
    }
 ```

 - Perform captured requester testing (including direct handlers invocations):
 ```
    RequesterCapture<MyPalyload> requesterCapture =
            TestMsbStorageForObjectFactory.getRequesterCapture("my:namespace:out");
    assertEquals(MyPalyload.class, requesterCapture.getPayloadClass());

    //invoke onEnd handler
    Callback<Void> onEndHandler = requesterCapture.getOnEndCaptor().getValue();
    onEndHandler.call(null);

    BiConsumer<MyPalyload, MessageContext> onResponseHandler = requesterCapture.getOnResponseCaptor().getValue();
    onResponseHandler.accept(myPalyload, messageContextMock);
 ```

  - Perform captured responder testing (including direct handlers invocations):
  ```
    ResponderCapture<MyPalyload> responderCapture = TestMsbStorageForObjectFactory.getResponderCapture("my:namespace:in");
    assertEquals(MyPalyload.class, responderCapture.getPayloadClass());

    //invoke requestHandler
    ResponderServer.RequestHandler<MyPalyload> requestHandler = responderCapture.getRequestHandler();
    requestHandler.process(myPalyload, responderContextMock);
  ```

 - Perform test data cleanup after each test method:
 ```
    @After
    public void tearDown() throws Exception {
        TestMsbStorageForObjectFactory.cleanup();
    }
 ```

## AdapterFactory-based approach
**Related package:** _io.github.tcdl.msb.testsupport.adapterfactory_

This type of test support is used to test the flow of an incoming message through all MSB layers.
Using this option makes it possible both submit a raw message JSON into a namespace, and capture outgoing messages raw JSON.

Typical usage:
 - Configure Msb to use the test AdapterFactory implementation:
 ```
 msbConfig {
   brokerAdapterFactory = "io.github.tcdl.msb.testsupport.adapterfactory.TestMsbAdapterFactory"
 }
 ```
 - Inject non-mocked MsbContext that is using this configuration.

 - Submit incoming messages JSON using:
  ```
  TestMsbStorageForAdapterFactory.publishIncomingMessage("my:incoming:namespace", "{my: 'message_json'}");
  ```
 - Verify outgoing message JSON using:
 ```
  List<String> publishedTestMessages = TestMsbStorageForAdapterFactory.getOutgoingMessages("my:out:namespace");
  String publishedMessage = TestMsbStorageForAdapterFactory.getOutgoingMessage("my:out:namespace");
 ```
 - During the testing, perform JSON String - Message conversions using MSB utilities if required:
 ```
 Utils.fromJson(json, Message.class, objectMapper);
 Utils.toJson(message, objectMapper);
 ```
 - Perform test data cleanup after each test method:
 ```
 @After
 public void tearDown() throws Exception {
     TestMsbStorageForAdapterFactory.cleanup();
 }
 ```
