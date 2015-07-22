package io.github.tcdl.msb.examples;

import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.examples.payload.Request;

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

    final Pattern YEAR_PATTERN = Pattern.compile("^\\D*(20(\\d{2})).*$");

    public static void main(String... args) {
        MsbContext msbContext = new MsbContextBuilder()
                .withDefaultChannelMonitorAgent(true)
                .withShutdownHook(true)
                .build();
        new DateExtractor().start(msbContext);
    }

    public void start(MsbContext msbContext) {
        MessageTemplate messageTemplate = new MessageTemplate();
        final String namespace = "search:parsers:facets:v1";

        msbContext.getObjectFactory().createResponderServer(namespace, messageTemplate, (request, responder) -> {

            Request dateRequest = (Request) request;
            String  queryString= dateRequest.getQuery().getQ();
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
                Payload responsePayload = new Payload.Builder()
                        .withStatusCode(200)
                        .withBody(responseBody).build();

                responder.send(responsePayload);
            }
        }, Request.class)
        .listen();
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

    private static  class Result {
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

        public static class Date {
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
