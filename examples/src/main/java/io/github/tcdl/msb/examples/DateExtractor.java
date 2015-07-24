package io.github.tcdl.msb.examples;

import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.examples.payload.Query;
import io.github.tcdl.msb.examples.payload.Request;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple example of date parser micro-service
 * It listens requests from facets-aggregator and parses year from query string
 * <p>
 * Created by rdro on 5/20/2015.
 */
public class DateExtractor {

    public static void main(String... args) {
        MsbContext msbContext = new MsbContextBuilder()
                .enableChannelMonitorAgent(true)
                .enableShutdownHook(true)
                .build();
        new DateExtractor().start(msbContext);
    }

    public void start(MsbContext msbContext) {
        MessageTemplate messageTemplate = new MessageTemplate();
        final String namespace = "search:parsers:facets:v1";

        msbContext.getObjectFactory().createResponderServer(namespace, messageTemplate, (request, responder) -> {

            Query query = request.getQuery();
            String queryString = query.getQ();
            String year = DateExtractorUtils.retrieveYear(queryString);

            if (year != null) {
                // send acknowledge
                responder.sendAck(500, null);

                // populate response body
                Result result = new Result();
                result.setStr(year);
                result.setStartIndex(queryString.indexOf(year));
                result.setEndIndex(queryString.indexOf(year) + year.length() - 1);
                result.setInferredDate(new HashMap<>());
                result.setProbability(0.9f);

                Result.Date date = new Result.Date();
                date.setYear(Integer.parseInt(year.substring(2, year.length())));
                result.setDate(date);

                ResponseBody responseBody = new ResponseBody();
                responseBody.setResults(Arrays.asList(result));
                Payload responsePayload = new Payload.Builder()
                        .withStatusCode(200)
                        .withBody(responseBody).build();

                responder.send(responsePayload);
            }
        }, Request.class).listen();
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