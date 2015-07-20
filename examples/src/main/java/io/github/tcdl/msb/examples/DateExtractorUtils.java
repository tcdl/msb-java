package io.github.tcdl.msb.examples;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateExtractorUtils {
    private static final Pattern DATE_PATTERN =
            Pattern.compile(".*?(((0?[1-9]|[12][0-9]|3[01])(/|\\.))?((0?[1-9]|1[012])(/|\\.))?((19|20)\\d\\d)).*");

    public static String retrieveYear(String s) {
        Matcher dateMatcher = DATE_PATTERN.matcher(s);
        if (dateMatcher.matches()) {
            String str = dateMatcher.group(1);
            if (str.contains(".")) {
                return str.split("\\.")[str.split("\\.").length - 1];
            } else if (str.contains("/")) {
                return str.split("/")[str.split("/").length - 1];
            } else {
                return str;
            }
        }
        return null;
    }
}
