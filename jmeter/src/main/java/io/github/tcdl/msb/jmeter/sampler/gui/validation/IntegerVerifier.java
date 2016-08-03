package io.github.tcdl.msb.jmeter.sampler.gui.validation;

import org.apache.commons.lang3.StringUtils;

import javax.swing.*;

/**
 * Created by rdro-tc on 28.07.16.
 */
public class IntegerVerifier extends InputVerifier {

    private boolean required = false;

    public IntegerVerifier(boolean required) {
        this.required = required;
    }

    public boolean verify(JComponent input) {
        JTextField textField = (JTextField) input;

        if (required && StringUtils.isBlank(textField.getText())) {
            JOptionPane.showMessageDialog(input,
                    "Required field: " + textField.getName(), "Validation Error",
                    JOptionPane.ERROR_MESSAGE);
            return false;
        }

        try {
            Integer.valueOf(textField.getText());
        } catch (NumberFormatException e) {
            JOptionPane.showMessageDialog(input,
                    "Invalid number: " + textField.getText(), "Validation Error",
                    JOptionPane.ERROR_MESSAGE);
            return false;
        }

        return true;
    }
}
