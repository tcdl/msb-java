package io.github.tcdl.examples;

import io.github.tcdl.Requester;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Created by anstr on 6/9/2015.
 */
public class SimpleRequesterExample  {
    private static final Integer NUMBER_OF_RESPONSES = 1;

    private boolean passed;
    private String namespace;
    private MSBUtil util = MSBUtil.getInstance();

    SimpleRequesterExample(String namespace) {
        this.namespace = namespace;
    }

    public boolean isPassed() {
        return passed;
    }

    public void runSimpleRequesterExample(String... expectedResponses) throws Exception {
        Requester requester = util.createRequester(namespace, NUMBER_OF_RESPONSES);

        passed = ArrayUtils.isEmpty(expectedResponses);

        util.sendRequest(requester, NUMBER_OF_RESPONSES, response -> {
            String body = response.getBody().toString();
            for (String bodyFragment : expectedResponses) {
                if (body.contains(bodyFragment)) {
                    passed = true;
                }
            }
        });
    }

    public void shutdown() {
        util.shutdown();
    }
}
