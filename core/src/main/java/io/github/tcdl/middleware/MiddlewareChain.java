package io.github.tcdl.middleware;

import io.github.tcdl.Responder;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by rdro on 4/29/2015.
 */
public class MiddlewareChain {

    private static final Logger LOG = LoggerFactory.getLogger(MiddlewareChain.class);

    private List<Middleware> middlewareList = new LinkedList<>();
    private Iterator<Middleware> iterator;
    private MiddlewareHandler errorHandler;

    public void add(Middleware... middleware) {
        middlewareList.addAll(Arrays.asList(middleware));
    }

    public MiddlewareChain invoke(Payload request, Responder responder) {
        if (!middlewareList.isEmpty()) {
            iterator = middlewareList.iterator();
            Middleware middleware = iterator.next();
            try {
                middleware.execute(request, responder, this);
            } catch (Exception e) {
                LOG.error("Error while processing request: {}", e.getMessage());
                if (errorHandler != null) {
                    errorHandler.handle(request, responder, e);
                }
            }
        }
        return this;
    }

    public void execute(Payload request, Responder responder) {
        iterator = Utils.ifNull(iterator, middlewareList.iterator());

        if (iterator.hasNext()) {
            Middleware middleware = iterator.next();
            try {
                middleware.execute(request, responder, this);
            } catch (Exception e) {
                LOG.error("Error while processing request: {}", e.getMessage());
                if (errorHandler != null) {
                    errorHandler.handle(request, responder, e);
                }
            }
        }
    }

    public MiddlewareChain withErrorHandler(MiddlewareHandler handler) {
        this.errorHandler = handler;
        return this;
    }
}
