package tcdl.msb.middleware;

import tcdl.msb.Response;
import tcdl.msb.events.EventHandler;
import tcdl.msb.messages.payload.BasicPayload;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by rdro on 4/29/2015.
 */
public class MiddlewareChain {

    private List<Middleware> middlewareList = new LinkedList<>();
    private Iterator<Middleware> iterator;
    private EventHandler handler;

    public void add(Middleware... middleware) {
        middlewareList.addAll(Arrays.asList(middleware));
    }

    public MiddlewareChain invoke(BasicPayload request, Response response) {
        if (!middlewareList.isEmpty()) {
            iterator = middlewareList.iterator();
            Middleware middleware = iterator.next();
            try {
                middleware.execute(request, response, this);
            } catch (Exception e) {
                if (handler != null) {
                    handler.onEvent(request, response, e);
                }
                // TODO log exception
            }
        }
        return this;
    }

    public void execute(BasicPayload request, Response response) {
        if (iterator.hasNext()) {
            Middleware middleware = iterator.next();
            try {
                middleware.execute(request, response, this);
            } catch (Exception e) {
                if (handler != null) {
                    handler.onEvent(request, response, e);
                }
                // TODO log exception
            }
        }
    }

    public MiddlewareChain withErrorHandler(EventHandler handler) {
        this.handler = handler;
        return this;
    }
}
