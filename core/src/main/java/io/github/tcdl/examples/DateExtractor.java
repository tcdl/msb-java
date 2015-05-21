package io.github.tcdl.examples;

import io.github.tcdl.Responder;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.payload.Payload;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rdro on 5/20/2015.
 */
public class DateExtractor {

    public static void main(String... args) {

        MsbMessageOptions options = new MsbMessageOptions();
        options.setNamespace("search:parsers:facets:v1");

        final Pattern YEAR_PATTERN = Pattern.compile("^.*(20(\\d{2})).*$");

        Responder.createServer(options)
                .use(((request, response) -> {
                    String queryString = (String)request.getQuery().get("q");
                    Matcher matcher = YEAR_PATTERN.matcher(queryString);

                    if (matcher.matches()) {
                        // send acknowledge
                        response.getResponder().sendAck(500, null, null);

                        // parse year
                        String str = matcher.group(1);
                        Integer year = Integer.valueOf(matcher.group(2));

                        Map<String, Object> results = new HashMap<>();
                        results.put("str", str);
                        results.put("startIndex", queryString.indexOf(str));
                        results.put("endIndex", queryString.indexOf(str) + str.length()-1);
                        {
                            Map<String, Object> date = new HashMap<>();
                            date.put("year", year);
                            results.put("date", date);
                        }
                        results.put("inferredDate", new HashMap<>());
                        results.put("probability", 0.9);

                        // send response
                        Map<String, Object> body = new HashMap<>();
                        body.put("results", new Object[] {results});
                        Payload responsePayload = new Payload.PayloadBuilder().setBody(body).build();

                        response.getResponder().send(responsePayload, null);
                    }
                }))
                .listen();
    }
}
