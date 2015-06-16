package io.github.tcdl.examples;

import io.github.tcdl.Requester;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

/**
 * Created by anstr on 6/9/2015.
 */
public class SimpleRequesterExample extends BaseExample {

    private boolean passed;

    private String namespace;

    SimpleRequesterExample(String namespace) {
        this.namespace = namespace;
    }

    public boolean isPassed() {
        return passed;
    }

    public void runSimpleRequesterExample(String... expectedResponses) throws Exception {
        Requester requester = createRequester(namespace, 1);

        passed = ArrayUtils.isEmpty(expectedResponses);

        sendRequest(requester, 1, response -> {
            String body = response.getBody().toString();
            for (String bodyFragment : expectedResponses) {
                if (body.contains(bodyFragment)) {
                    passed = true;
                }
            }
        });
    }
}
