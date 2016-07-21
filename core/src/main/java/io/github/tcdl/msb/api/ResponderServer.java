package io.github.tcdl.msb.api;

import io.github.tcdl.msb.api.message.Message;

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
     * Stop listening
     */
    ResponderServer stop();

    /**
     * Implementation of this interface contains business logic processed by microservice.
     */
    interface RequestHandler<T> {
        /**
         * Execute business logic and send response.
         * @param request request received from a bus
         * @param responderContext object of type {@link ResponderContext} which will 
         * provide access to {@link Responder} that used for sending response and 
         * {@link AcknowledgementHandler} that used for explicit confirm/reject received request
         * @throws Exception if some problems during execution business logic or sending response were occurred
         */
        void process(T request, ResponderContext responderContext) throws Exception;
    }

    /**
     * Implementation of this interface contains custom error handler
     */
    interface ErrorHandler {
        /**
         * Executes user defined error handler
         *
         * @param exception error cause
         * @param originalMessage original message
         */
        void handle(Exception exception, Message originalMessage);
    }
}
