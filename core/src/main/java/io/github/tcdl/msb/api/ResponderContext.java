package io.github.tcdl.msb.api;


/**
 * Provides access to Responder Context.
 */
public interface ResponderContext extends MessageContext {

    /**
     * @return Responder instance
     */
    Responder getResponder();
    
}
