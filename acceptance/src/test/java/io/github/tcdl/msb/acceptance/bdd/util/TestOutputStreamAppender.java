package io.github.tcdl.msb.acceptance.bdd.util;

import ch.qos.logback.core.OutputStreamAppender;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * This appender writes log messages into an internal buffer
 * so it is possible to query log for particular messages
 * during integration testing.
 */
public class TestOutputStreamAppender<E> extends OutputStreamAppender<E> {

    private final static ByteArrayOutputStream OUT = new ByteArrayOutputStream();
    private final static BufferedOutputStream OUT_BUFFERED = new BufferedOutputStream(OUT);

    /**
     * Clean an internal text buffer.
     * @throws Exception
     */
    public synchronized static void reset() throws Exception {
        OUT_BUFFERED.flush();
        OUT.flush();
        OUT.reset();
    }

    /**
     * Substring lookup in an internal text buffer.
     * @param substring
     * @param retries
     * @param pollIntervalMs
     * @return
     * @throws Exception
     */
    public synchronized static boolean isPresent(String substring, int retries, long pollIntervalMs) throws Exception {
        do {
            if(OUT.toString().contains(substring)) {
                return true;
            }
            Thread.sleep(pollIntervalMs);
        } while (retries --> 0);
        return false;
    }

    @Override
    public void start() {
        setOutputStream(OUT_BUFFERED);
        super.start();
    }

    @Override
    public void stop() {
        try {
            OUT_BUFFERED.close();
            OUT.close();
        } catch (IOException ex) {
            //ignore
        }
        super.stop();
    }

    @Override
    protected void writeOut(E event) throws IOException {
        super.writeOut(event);
    }
}
