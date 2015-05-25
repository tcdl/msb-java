package io.github.tcdl.adapters.mock;

import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.AdapterBuilder;
import io.github.tcdl.config.MsbConfigurations;

/**
 * Implementation of {@link AdapterBuilder} for MockAdapter
 * @author ysavchuk
 *
 */
public class MockAdapterBuilder implements AdapterBuilder {
    private String topic; 
    
    @Override
    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public void setMsbConfigurations(MsbConfigurations msbConfig) {
    }

    @Override
    public Adapter createAdapter() {
        return new MockAdapter(topic, null);
    }
    
    

}
