package io.github.tcdl.msb.jmeter.sampler.gui.validation;

import org.apache.commons.lang3.StringUtils;

import javax.swing.*;
import java.util.regex.Pattern;

/**
 * Created by rdro-tc on 02.08.16.
 */
public class PatternVerifier extends IntegerVerifier {

    private boolean required = false;
    private Pattern pattern;

    public PatternVerifier(boolean required, String pattern) {
        super(required);
        this.pattern = Pattern.compile(pattern);
    }

    public boolean verify(JComponent input) {
        JTextField textField = (JTextField) input;

        if (required && StringUtils.isBlank(textField.getText())) {
            JOptionPane.showMessageDialog(input,
                    "Required field: " + textField.getName(), "Validation Error",
                    JOptionPane.ERROR_MESSAGE);

            return false;
        }

        if (StringUtils.isNotBlank(textField.getText()) && !pattern.matcher(textField.getText()).matches()) {
            JOptionPane.showMessageDialog(input,
                    "Invalid " + textField.getName(), "Validation Error",
                    JOptionPane.ERROR_MESSAGE);

            return false;
        }

        return true;
    }
}
