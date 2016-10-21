package io.github.tcdl.msb.api;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.github.tcdl.msb.api.monitor.AggregatorStats;
import io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator;

import java.lang.reflect.Type;
import java.util.Set;

/**
 * Provides methods for creation client-facing API objects.
 */
public interface ObjectFactory {

    /**
     * Convenience method that specifies response payload type as {@link JsonNode}
     *
     * See {@link #createRequester(String, RequestOptions, TypeReference)}
     */
    default Requester<JsonNode> createRequester(String namespace, RequestOptions requestOptions) {
        return createRequester(namespace, requestOptions, JsonNode.class);
    }

    /**
     * Convenience method that allows to specify response payload type via {@link Class}
     *
     * See {@link #createRequester(String, RequestOptions, TypeReference)}
     */
    default <T> Requester<T> createRequester(String namespace, RequestOptions requestOptions, Class<T> payloadClass) {
        return createRequester(namespace, requestOptions, new TypeReference<T>() {
            @Override
            public Type getType() {
                return payloadClass;
            }
        });
    }

    /**
     * @param namespace            topic name to send a request to
     * @param requestOptions       options to configure a requester
     * @param payloadTypeReference expected payload type of response messages
     * @return new instance of a {@link Requester}
     */
    <T> Requester<T> createRequester(String namespace, RequestOptions requestOptions, TypeReference<T> payloadTypeReference);

    /**
     * Same as
     * {@link #createRequesterForSingleResponse(String, Class, RequestOptions)}
     * with default request options
     */
    default <T> Requester<T> createRequesterForSingleResponse(String namespace, Class<T> payloadClass) {
        return createRequesterForSingleResponse(namespace, payloadClass, RequestOptions.DEFAULTS);
    }

    /**
     * Creates requester for single response
     *
     * @param namespace          topic name to send a request to
     * @param payloadClass       expected payload class of response messages
     * @param baseRequestOptions request options to be used as a source of response timeout and {@link MessageTemplate}.
     *                           Response time however will be 1 even if {@code baseRequestOptions} define other value.
     * @return new instance of a {@link Requester}
     */
    <T> Requester<T> createRequesterForSingleResponse(String namespace, Class<T> payloadClass, RequestOptions baseRequestOptions);

    /**
     * Convenience method that specifies incoming payload type as {@link JsonNode}
     *
     * @deprecated use {@link #createResponderServer(String, ResponderOptions, ResponderServer.RequestHandler, ResponderServer.ErrorHandler, TypeReference)}
     */
    @Deprecated
    default ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
                                                  ResponderServer.RequestHandler<JsonNode> requestHandler) {
        return createResponderServer(namespace, new ResponderOptions.Builder().withMessageTemplate(messageTemplate).build(), requestHandler, JsonNode.class);
    }

    /**
     * Convenience method that allows to specify incoming payload type via {@link Class}
     *
     * @deprecated use {@link #createResponderServer(String, ResponderOptions, ResponderServer.RequestHandler, ResponderServer.ErrorHandler, TypeReference)}
     */
    @Deprecated
    default <T> ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
                                                      ResponderServer.RequestHandler<T> requestHandler, Class<T> payloadClass) {
        ResponderOptions responderOptions = new ResponderOptions.Builder().withMessageTemplate(messageTemplate).build();
        return createResponderServer(namespace, responderOptions, requestHandler, new TypeReference<T>() {
            @Override
            public Type getType() {
                return payloadClass;
            }
        });
    }

    /**
     * @param namespace            topic on a bus for listening on incoming requests
     * @param responderOptions     {@link ResponderOptions} to be used
     * @param requestHandler       handler for processing the request
     * @param errorHandler         handler for errors to be called after default
     * @param payloadTypeReference expected payload type of incoming messages
     * @return new instance of a {@link ResponderServer} that unmarshals payload into specified payload type
     */
    <T> ResponderServer createResponderServer(String namespace, ResponderOptions responderOptions,
                                              ResponderServer.RequestHandler<T> requestHandler, ResponderServer.ErrorHandler errorHandler, TypeReference<T> payloadTypeReference);

    /**
     * Convenience method that specifies incoming payload type as {@link JsonNode}
     * <p>
     * See {@link #createResponderServer(String, ResponderOptions, ResponderServer.RequestHandler, ResponderServer.ErrorHandler, TypeReference)}
     */
    default ResponderServer createResponderServer(String namespace, ResponderOptions responderOptions,
                                                  ResponderServer.RequestHandler<JsonNode> requestHandler, ResponderServer.ErrorHandler errorHandler) {
        return createResponderServer(namespace, responderOptions, requestHandler, errorHandler, new TypeReference<JsonNode>() {
            @Override
            public Type getType() {
                return JsonNode.class;
            }
        });
    }

    /**
     * See {@link #createResponderServer(String, ResponderOptions, ResponderServer.RequestHandler, ResponderServer.ErrorHandler, TypeReference)}
     */
    default <T> ResponderServer createResponderServer(String namespace, ResponderOptions responderOptions,
                                                      ResponderServer.RequestHandler<T> requestHandler, TypeReference<T> payloadTypeReference) {
        return createResponderServer(namespace, responderOptions, requestHandler, null, payloadTypeReference);
    }

    /**
     * See {@link #createResponderServer(String, ResponderOptions, ResponderServer.RequestHandler, ResponderServer.ErrorHandler, TypeReference)}
     */
    default <T> ResponderServer createResponderServer(String namespace, ResponderOptions responderOptions,
                                                      ResponderServer.RequestHandler<T> requestHandler, Class<T> payloadClass) {
        return createResponderServer(namespace, responderOptions, requestHandler, new TypeReference<T>() {
            @Override
            public Type getType() {
                return payloadClass;

            }
        });
    }

    default <T> ResponderServer createResponderServer(String namespace, ResponderOptions responderOptions,
                                                      ResponderServer.RequestHandler<T> requestHandler, ResponderServer.ErrorHandler errorHandler, Class<T> payloadClass) {
        return createResponderServer(namespace, responderOptions, requestHandler, errorHandler, new TypeReference<T>() {
            @Override
            public Type getType() {
                return payloadClass;

            }
        });
    }

    /**
     * @deprecated use {@link #createResponderServer(String, ResponderOptions, ResponderServer.RequestHandler, ResponderServer.ErrorHandler, TypeReference)}
     */
    @Deprecated
    default <T> ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
                                                      ResponderServer.RequestHandler<T> requestHandler, ResponderServer.ErrorHandler errorHandler, TypeReference<T> payloadTypeReference) {
        ResponderOptions responderOptions = new ResponderOptions.Builder().withMessageTemplate(messageTemplate).build();
        return createResponderServer(namespace, responderOptions, requestHandler, errorHandler, payloadTypeReference);
    }

    /**
     * @deprecated use {@link #createResponderServer(String, ResponderOptions, ResponderServer.RequestHandler, ResponderServer.ErrorHandler, TypeReference)}
     */
    @Deprecated
    default <T> ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
                                                      ResponderServer.RequestHandler<T> requestHandler, ResponderServer.ErrorHandler errorHandler, Class<T> payloadClass) {
        ResponderOptions responderOptions = new ResponderOptions.Builder().withMessageTemplate(messageTemplate).build();
        return createResponderServer(namespace, responderOptions, requestHandler, errorHandler, new TypeReference<T>() {
            @Override
            public Type getType() {
                return payloadClass;
            }
        });
    }

    /**
     * See {@link #createRequesterForFireAndForget(java.lang.String, io.github.tcdl.msb.api.RequestOptions)}
     */
    default <T> Requester<T> createRequesterForFireAndForget(String namespace){
        return createRequesterForFireAndForget(namespace, RequestOptions.DEFAULTS);
    }

    /**
     * Creates requester that doesn't wait for any responses or acknowledgments
     *
     * @return new instance of a {@link Requester} with original message
     */
    <T> Requester<T> createRequesterForFireAndForget(String namespace, RequestOptions requestOptions);

    /**
     * @deprecated use {@link ObjectFactory#createResponderServer(java.lang.String, io.github.tcdl.msb.api.ResponderOptions, io.github.tcdl.msb.api.ResponderServer.RequestHandler, io.github.tcdl.msb.api.ResponderServer.ErrorHandler, com.fasterxml.jackson.core.type.TypeReference)}
     */
    @Deprecated
    default <T> ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
                                                      ResponderServer.RequestHandler<T> requestHandler, TypeReference<T> payloadTypeReference) {
        ResponderOptions responderOptions = new ResponderOptions.Builder().withMessageTemplate(messageTemplate).build();
        return createResponderServer(namespace, responderOptions, requestHandler, payloadTypeReference);
    }

    /**
     * @return instance of converter to convert any objects
     * using object mapper from {@link MsbContext}
     */
    PayloadConverter getPayloadConverter();

    /**
     * @param aggregatorStatsHandler this handler is invoked whenever statistics is updated via announcement channel or heartbeats.
     *                               THE HANDLER SHOULD BE THREAD SAFE because it may be invoked from parallel threads.
     * @return new instance of {@link ChannelMonitorAggregator}
     */
    ChannelMonitorAggregator createChannelMonitorAggregator(Callback<AggregatorStats> aggregatorStatsHandler);

    /**
     * Shuts down the factory and all the objects that were created by it.
     */
    void shutdown();
}