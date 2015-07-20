package io.github.tcdl.msb.examples;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DateExtractorUtilsTest {

    @Test
    public void testOnlyYear() {
        assertEquals("2015", DateExtractorUtils.retrieveYear("2015"));
    }

    @Test
    public void testOnlyYearSpaces() {
        assertEquals("2015", DateExtractorUtils.retrieveYear(" 2015 "));
    }

    @Test
    public void testMonthYear() {
        assertEquals("2015", DateExtractorUtils.retrieveYear("02/2015"));
    }

    @Test
    public void testMonthYearDot() {
        assertEquals("2015", DateExtractorUtils.retrieveYear("02.2015"));
    }

    @Test
    public void testDayMonthYear() {
        assertEquals("2015", DateExtractorUtils.retrieveYear("12/03/2015"));
    }

    @Test
    public void testDayMonthYearDot() {
        assertEquals("2015", DateExtractorUtils.retrieveYear("12.03.2015"));
    }

    @Test
    public void testDateTextStart() {
        assertEquals("2015", DateExtractorUtils.retrieveYear("12/03/2015 London holidays"));
    }

    @Test
    public void testDateTextStartEnd() {
        assertEquals("2015", DateExtractorUtils.retrieveYear("Hotels 12/03/2015 London holidays"));
    }

    @Test
    public void testDateTextStartEndDot() {
        assertEquals("2015", DateExtractorUtils.retrieveYear("Hotels 12.03.2015 London holidays"));
    }

    @Test
    public void testDateSpaceAtTheBeginning() {
        assertEquals("2015", DateExtractorUtils.retrieveYear(" 12/03/2015 London holidays"));
    }

    @Test
    public void testDateSpaceAtEnd() {
        assertEquals("2015", DateExtractorUtils.retrieveYear("12/03/2015 London holidays "));
    }

    @Test
    public void testDateSpaceAtTheBeginningEnd() {
        assertEquals("2015", DateExtractorUtils.retrieveYear(" 12/03/2015 London holidays "));
    }

    @Test
    public void testDateYear() {
        assertEquals("2015", DateExtractorUtils.retrieveYear("London 03.03.2015-counter-2078"));
    }

    @Test
    public void testTwoYears() {
        assertEquals("2015", DateExtractorUtils.retrieveYear("London 2015-counter-2078"));
    }
}