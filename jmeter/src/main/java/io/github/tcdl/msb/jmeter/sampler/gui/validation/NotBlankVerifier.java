package io.github.tcdl.msb.jmeter.sampler.gui.validation;

import org.apache.commons.lang3.StringUtils;

import javax.swing.*;

/**
 * Created by rdro-tc on 02.08.16.
 */
public class NotBlankVerifier extends InputVerifier {

    public boolean verify(JComponent input) {
        JTextField textField = (JTextField) input;

        if (StringUtils.isBlank(textField.getText())) {
            JOptionPane.showMessageDialog(input,
                    "Required field: " + textField.getName(), "Validation Error",
                    JOptionPane.ERROR_MESSAGE);

            return false;
        }

        return true;
    }
}
