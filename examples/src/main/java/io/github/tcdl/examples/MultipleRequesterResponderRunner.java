package io.github.tcdl.examples;

/**
 * Created by anstr on 6/10/2015.
 */
public class MultipleRequesterResponderRunner {
    public static void main(String[] args) {
        SimpleResponderExample.main("test:simple-queue2");
        SimpleResponderExample.main("test:simple-queue3");

        MultipleRequesterResponder.main("test:simple-queue1", "test:simple-queue2", "test:simple-queue3");

        SimpleRequesterExample.main("test:simple-queue1");
    }
}
