package io.github.tcdl.msb.jmeter.sampler.gui;

import io.github.tcdl.msb.jmeter.sampler.RequesterConfig;
import io.github.tcdl.msb.jmeter.sampler.gui.validation.IntegerVerifier;
import io.github.tcdl.msb.jmeter.sampler.gui.validation.JsonVerifier;
import io.github.tcdl.msb.jmeter.sampler.gui.validation.NotBlankVerifier;
import io.github.tcdl.msb.jmeter.sampler.gui.validation.PatternVerifier;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

/**
 * Created by rdro-tc on 28.07.16.
 */
public class RequesterConfigForm {

    private JPanel configPanel;
    private JPanel brokerConfigPanel;
    private JPanel requesterConfigPanel;
    private JScrollPane requestPayloadPanel;

    private JLabel hostLabel;
    private JLabel portLabel;
    private JLabel virtualHostLabel;
    private JLabel userNameLabel;
    private JLabel passwordLabel;
    private JTextField hostField;
    private JTextField portField;
    private JTextField virtualHostField;
    private JTextField userNameField;
    private JTextField passwordField;

    private JLabel namespaceLabel;
    private JLabel forwardNamespaceLabel;
    private JLabel numberOfResponsesLabel;
    private JLabel timeoutLabel;
    private JTextField namespaceField;
    private JTextField forwardNamespaceField;
    private JTextField numberOfResponsesField;
    private JTextField timeoutField;
    private JCheckBox waitForResponsesCheckBox;

    private JTextArea requestPayloadField;

    public RequesterConfigForm(RequesterConfig config) {
        setupUI();
        setConfig(config);
        waitForResponsesCheckBox.addItemListener(new ItemListener() {
            public void itemStateChanged(ItemEvent e) {
                onResponseEnabled(e.getStateChange() == ItemEvent.SELECTED);
            }
        });

        hostField.setInputVerifier(new NotBlankVerifier());
        portField.setInputVerifier(new IntegerVerifier(true));
        virtualHostField.setInputVerifier(new NotBlankVerifier());
        userNameField.setInputVerifier(new NotBlankVerifier());
        passwordField.setInputVerifier(new NotBlankVerifier());

        namespaceField.setInputVerifier(new PatternVerifier(true, "^_?([a-z0-9\\-]+\\:)+([a-z0-9\\-]+)$"));
        forwardNamespaceField.setInputVerifier(new PatternVerifier(false, "^_?([a-z0-9\\-]+\\:)+([a-z0-9\\-]+)$"));
        numberOfResponsesField.setInputVerifier(new IntegerVerifier(true));
        timeoutField.setInputVerifier(new IntegerVerifier(true));
        requestPayloadField.setInputVerifier(new JsonVerifier(true));
    }

    public JComponent getUIComponent() {
        return configPanel;
    }

    private void onResponseEnabled(boolean enabled) {
        numberOfResponsesField.setEnabled(enabled);
        timeoutField.setEnabled(enabled);
    }

    public void setConfig(RequesterConfig config) {
        hostField.setText(config.getHost());
        portField.setText(config.getPort() != null ? config.getPort().toString() : "");
        virtualHostField.setText(config.getVirtualHost());
        userNameField.setText(config.getUserName());
        passwordField.setText(config.getPassword());

        namespaceField.setText(config.getNamespace());
        forwardNamespaceField.setText(config.getForwardNamespace());
        numberOfResponsesField.setText(config.getNumberOfResponses() != null ? config.getNumberOfResponses().toString() : "");
        timeoutField.setText(config.getTimeout() != null ? config.getTimeout().toString() : "");
        requestPayloadField.setText(config.getRequestPayload());
        waitForResponsesCheckBox.setSelected(config.getWaitForResponses());
    }

    public RequesterConfig getConfig() {
        RequesterConfig config = new RequesterConfig();

        config.setHost(hostField.getText());
        config.setPort(Integer.valueOf(portField.getText()));
        config.setVirtualHost(virtualHostField.getText());
        config.setUserName(userNameField.getText());
        config.setPassword(passwordField.getText());

        config.setNamespace(namespaceField.getText());
        config.setForwardNamespace(forwardNamespaceField.getText());
        config.setTimeout(Integer.valueOf(timeoutField.getText()));
        config.setRequestPayload(requestPayloadField.getText());
        config.setNumberOfResponses(Integer.valueOf(numberOfResponsesField.getText()));
        config.setWaitForResponses(waitForResponsesCheckBox.isSelected());

        return config;
    }

    private void setupUI() {
        configPanel = new JPanel();
        configPanel.setLayout(new GridBagLayout());

        brokerConfigPanel = new JPanel();
        brokerConfigPanel.setLayout(new GridBagLayout());
        brokerConfigPanel.setBorder(BorderFactory.createTitledBorder("Broker configuration"));

        hostLabel = new JLabel();
        hostLabel.setText("host");
        brokerConfigPanel.add(hostLabel, constraints(0, 0));
        hostField = new JTextField();
        hostField.setName("host");
        brokerConfigPanel.add(hostField, constraints(1, 0));

        portLabel = new JLabel();
        portLabel.setText("port");
        brokerConfigPanel.add(portLabel, constraints(0, 1));
        portField = new JTextField();
        portField.setName("port");
        brokerConfigPanel.add(portField, constraints(1, 1));

        virtualHostLabel = new JLabel();
        virtualHostLabel.setText("virtual host");
        brokerConfigPanel.add(virtualHostLabel, constraints(0, 2));
        virtualHostField = new JTextField();
        virtualHostField.setName("virtual host");
        brokerConfigPanel.add(virtualHostField, constraints(1, 2));

        userNameLabel = new JLabel();
        userNameLabel.setText("user name");
        brokerConfigPanel.add(userNameLabel, constraints(0, 3));
        userNameField = new JTextField();
        userNameField.setName("user name");
        brokerConfigPanel.add(userNameField, constraints(1, 3));

        passwordLabel = new JLabel();
        passwordLabel.setText("password");
        passwordLabel.setName("password");
        brokerConfigPanel.add(passwordLabel, constraints(0, 4));
        passwordField = new JTextField();
        passwordField.setName("password");
        brokerConfigPanel.add(passwordField, constraints(1, 4));
        configPanel.add(brokerConfigPanel, constraints(0, 0));

        requesterConfigPanel = new JPanel();
        requesterConfigPanel.setLayout(new GridBagLayout());
        requesterConfigPanel.setBorder(BorderFactory.createTitledBorder("Requester configuration"));

        namespaceLabel = new JLabel("namespace");
        requesterConfigPanel.add(namespaceLabel, constraints(0, 0));
        namespaceField = new JTextField();
        namespaceField.setName("namespace");
        requesterConfigPanel.add(namespaceField, constraints(1, 0));

        forwardNamespaceLabel = new JLabel("forward namespace");
        requesterConfigPanel.add(forwardNamespaceLabel, constraints(0, 1));
        forwardNamespaceField = new JTextField();
        forwardNamespaceField.setName("forward namespace");
        requesterConfigPanel.add(forwardNamespaceField, constraints(1, 1));

        numberOfResponsesLabel = new JLabel();
        numberOfResponsesLabel.setText("number of responses");
        requesterConfigPanel.add(numberOfResponsesLabel, constraints(0, 2));
        numberOfResponsesField = new JTextField();
        numberOfResponsesField.setName("number of responses");
        requesterConfigPanel.add(numberOfResponsesField, constraints(1, 2));

        timeoutLabel = new JLabel();
        timeoutLabel.setText("timeout, ms");
        requesterConfigPanel.add(timeoutLabel, constraints(0, 3));
        timeoutField = new JTextField();
        timeoutField.setName("timeout");
        requesterConfigPanel.add(timeoutField, constraints(1, 3));

        waitForResponsesCheckBox = new JCheckBox();
        waitForResponsesCheckBox.setEnabled(true);
        waitForResponsesCheckBox.setSelected(true);
        waitForResponsesCheckBox.setText("wait for responses");
        requesterConfigPanel.add(waitForResponsesCheckBox, constraints(0, 4));
        configPanel.add(requesterConfigPanel, constraints(0, 1));

        requestPayloadPanel = new JScrollPane();
        configPanel.add(requestPayloadPanel, constraints(0, 2));
        requestPayloadPanel.setBorder(BorderFactory.createTitledBorder("Request payload"));
        requestPayloadPanel.setPreferredSize(new Dimension(500, 300));
        requestPayloadField = new JTextArea();
        requestPayloadField.setName("request payload");
        requestPayloadPanel.setViewportView(requestPayloadField);
    }

    private GridBagConstraints constraints(int gridx, int gridy) {
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.LINE_START;
        c.gridx = gridx;
        c.gridy = gridy;
        c.weightx = 1;
        c.weighty = 1;
        return c;
    }
}
