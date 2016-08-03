package io.github.tcdl.msb.jmeter.sampler;

import io.github.tcdl.msb.jmeter.sampler.gui.RequesterConfigForm;

import javax.swing.*;

/**
 * Created by rdro-tc on 28.07.16.
 */
public class UiTest {

    public static void main(String[] args) {
        JFrame frame = new JFrame("Test");
        frame.setContentPane(new RequesterConfigForm(new RequesterConfig()).getUIComponent());
        frame.pack();
        frame.setVisible(true);
    }
}
