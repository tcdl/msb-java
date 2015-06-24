package io.github.tcdl.examples;

import io.github.tcdl.MsbContext;
import io.github.tcdl.ResponderServer;
import io.github.tcdl.config.MessageTemplate;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.monitor.DefaultChannelMonitorAgent;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple example of date parser micro-service
 * It listens requests from facets-aggregator and parses year from query string
 *
 * Created by rdro on 5/20/2015.
 */
public class DateExtractor {

    public static void main(String... args) {

        MsbContext msbContext = new MsbContext.MsbContextBuilder().
                withDefaultChannelMonitorAgent(true).
                withShutdownHook(true).
                build();

        MessageTemplate options = new MessageTemplate();
        final String namespace = "search:parsers:facets:v1";

        final Pattern YEAR_PATTERN = Pattern.compile("^.*(20(\\d{2})).*$");

        ResponderServer.create(namespace, options, msbContext, (request, responder) -> {

                    RequestQuery query = request.getQueryAs(RequestQuery.class);
                    String queryString = query.getQ();
                    Matcher matcher = YEAR_PATTERN.matcher(queryString);

                    if (matcher.matches()) {
                        // send acknowledge
                        responder.sendAck(500, null);

                        // parse year
                        String str = matcher.group(1);
                        Integer year = Integer.valueOf(matcher.group(2));

                        // populate response body
                        Result result = new Result();
                        result.setStr(str);
                        result.setStartIndex(queryString.indexOf(str));
                        result.setEndIndex(queryString.indexOf(str) + str.length() - 1);
                        result.setInferredDate(new HashMap<>());
                        result.setProbability(0.9f);

                        Result.Date date = new Result.Date();
                        date.setYear(year);
                        result.setDate(date);

                        ResponseBody responseBody = new ResponseBody();
                        responseBody.setResults(Arrays.asList(result));
                        Payload responsePayload = new Payload.PayloadBuilder()
                                .setStatusCode(200)
                                .setBody(responseBody).build();

                        responder.send(responsePayload);
                    }
                })
                .listen();
    }

    private static class RequestQuery {
        private String q;

        public String getQ() {
            return q;
        }

        public void setQ(String q) {
            this.q = q;
        }
    }

    private static class ResponseBody {
        private List<Result> results;

        public List<Result> getResults() {
            return results;
        }

        public void setResults(List<Result> results) {
            this.results = results;
        }
    }

    private static class Result {
        private String str;
        private int startIndex;
        private int endIndex;
        private Date date;
        private Map inferredDate;
        private float probability;

        public String getStr() {
            return str;
        }

        public void setStr(String str) {
            this.str = str;
        }

        public int getStartIndex() {
            return startIndex;
        }

        public void setStartIndex(int startIndex) {
            this.startIndex = startIndex;
        }

        public int getEndIndex() {
            return endIndex;
        }

        public void setEndIndex(int endIndex) {
            this.endIndex = endIndex;
        }

        public Date getDate() {
            return date;
        }

        public void setDate(Date date) {
            this.date = date;
        }

        public Map getInferredDate() {
            return inferredDate;
        }

        public void setInferredDate(Map inferredDate) {
            this.inferredDate = inferredDate;
        }

        public float getProbability() {
            return probability;
        }

        public void setProbability(float probability) {
            this.probability = probability;
        }

        private static class Date {
            private int year;

            public int getYear() {
                return year;
            }

            public void setYear(int year) {
                this.year = year;
            }
        }
    }
}
