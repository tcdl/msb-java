package io.github.tcdl.msb.jmeter.sampler.gui.validation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import io.github.tcdl.msb.support.Utils;
import org.apache.commons.lang3.StringUtils;

import javax.swing.*;
import java.util.Map;

/**
 * Created by rdro-tc on 02.08.16.
 */
public class JsonVerifier extends InputVerifier {

    private ObjectMapper objectMapper = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private boolean required = false;

    public JsonVerifier(boolean required) {
        this.required = required;
    }

    public boolean verify(JComponent input) {
        JTextArea textField = (JTextArea) input;

        if (required && StringUtils.isBlank(textField.getText())) {
            JOptionPane.showMessageDialog(input,
                    "Required field: " + textField.getName(), "Validation Error",
                    JOptionPane.ERROR_MESSAGE);
            return false;
        }

        try {
            Utils.fromJson(textField.getText(), Map.class, objectMapper);
        } catch (JsonConversionException e) {
            JOptionPane.showMessageDialog(input,
                    "Invalid json: " + textField.getText(),  "Validation Error",
                    JOptionPane.ERROR_MESSAGE);
            return false;
        }

        return true;
    }
}
