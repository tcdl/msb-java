package io.github.tcdl.msb.api;

/**
 * Callback interface for explicit message acknowledgement
 *
 */
public interface AcknowledgementHandler {
    
    /**
     * Set autoAcknowledgement value. 
     * @param autoAcknowledgement
     * If autoAcknowledgement is true:
     * 1. A message can be confirmed/rejected by microservice developer in {@link ResponderServer.process(()} 
     * or {@link Requester.onAcknowledge()}, {@link Requester.onResponse}, {@link Requester.onRawResponse()} methods. 
     * 2. If a message is not confirmed/rejected during a message processing, 
     * acknowledgement will be automatically sent just after completion these methods by rules: 
     * - message confirmed if message processed successfully,
     * - message declined if message has incorrect structure and can't be processed
     * - message rejected with requeue if error happens during processing
     * If autoAcknowledgement is false:
     * microservice developer MUST explicitly confirm/reject a message. 
     * autoAcknowledgement must be set to false if a message processing need to be continued in another thread. In this case
     * a message should be explicitly confirmed/rejected by microservice developer
     * autoAcknowledgement is true b default.
     */
    void setAutoAcknowledgement(boolean autoAcknowledgement);

    /**
     * @return current autoAcknowledgement value
     */
    boolean isAutoAcknowledgement();

    /**
     * Inform server that a message was confirmed by consumer. 
     * Server should consider message acknowledged once delivered
     */
    void confirmMessage();
    
    /**
     * Inform server that a message was rejected by consumer. 
     * AMQP Server may requeue message or delete it from queue depending on the
     * requeueRejectedMessages configuration option   
     */
    void rejectMessage();
    
    /**
     * Inform server that a message was rejected by consumer and can't be requeue  
     */    
    void discardMessage(); 
    
}
