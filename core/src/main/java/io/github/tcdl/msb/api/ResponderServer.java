package io.github.tcdl.msb.api;

/**
 * {@link ResponderServer} enable user to listen on messages from the bus and executing microservice business logic.
 * Call to {@link #listen()} method will start listening on incoming messages from the bus.
 * And also it's required to implement interface {@link RequestHandler}. Implementation of this interface will be
 * business logic processed by microservice. Inside this logic we can use instance of {@link Responder} created by {@code ResponderServer}
 * for each message from bus, and can be used for sending responses back to bus.
 */
public interface ResponderServer {

    int INTERNAL_SERVER_ERROR_CODE = 500;
    int PAYLOAD_CONVERSION_ERROR_CODE = 422;

    /**
     * Start listening for message on specified topic.
     */
     ResponderServer listen();

    /**
     * Implementation of this interface contains business logic processed by microservice.
     */
    interface RequestHandler<T> {
        /**
         * Execute business logic and send response.
         * @param request request received from a bus
         * @param responder object of type {@link Responder} which will be used for sending response
         * @throws Exception if some problems during execution business logic or sending response were occurred
         */
        void process(T request, Responder responder) throws Exception;
    }

}
