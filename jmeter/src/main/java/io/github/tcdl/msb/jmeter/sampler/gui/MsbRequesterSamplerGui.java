package io.github.tcdl.msb.jmeter.sampler.gui;

import io.github.tcdl.msb.jmeter.sampler.MsbRequesterSampler;
import io.github.tcdl.msb.jmeter.sampler.RequesterConfig;
import org.apache.jmeter.samplers.gui.AbstractSamplerGui;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.testelement.property.ObjectProperty;

import java.awt.*;

public class MsbRequesterSamplerGui extends AbstractSamplerGui {

    private RequesterConfigForm configForm;

    public MsbRequesterSamplerGui() {
        init();
    }

    public String getName() {
        return "MSB Requester Sampler";
    }

    public String getLabelResource() {
        return null;
    }

    public String getStaticLabel() {
        return "MSB Requester Sampler";
    }

    public void configure(TestElement testElement) {
        super.configure(testElement);

        MsbRequesterSampler sampler = (MsbRequesterSampler) testElement;
        RequesterConfig config = (RequesterConfig)sampler.getProperty(RequesterConfig.TEST_ELEMENT_CONFIG).getObjectValue();

        if (config == null) {
            sampler.setProperty(new ObjectProperty(RequesterConfig.TEST_ELEMENT_CONFIG, configForm.getConfig()));
        } else {
            configForm.setConfig(config);
        }

        sampler.init();
    }

    public TestElement createTestElement() {
        MsbRequesterSampler sampler = new MsbRequesterSampler();
        modifyTestElement(sampler);
        return sampler;
    }

    public void modifyTestElement(TestElement testElement) {
        configureTestElement(testElement);
        MsbRequesterSampler sampler = (MsbRequesterSampler) testElement;
        sampler.setProperty(new ObjectProperty(RequesterConfig.TEST_ELEMENT_CONFIG, configForm.getConfig()));
    }

    private void init() {
        setLayout(new BorderLayout(0, 0));
        setBorder(makeBorder());
        add(makeTitlePanel(), BorderLayout.NORTH);

        RequesterConfig config = new RequesterConfig();
        configForm = new RequesterConfigForm(config);
        add(configForm.getUIComponent(), BorderLayout.WEST);
    }

    @Override
    public void clearGui() {
        configForm.setConfig(new RequesterConfig());
        super.clearGui();
    }
}