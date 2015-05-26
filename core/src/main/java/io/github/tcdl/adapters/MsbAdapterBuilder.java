package io.github.tcdl.adapters;

import io.github.tcdl.config.MsbConfigurations;

/**
 * AdapterBuilder interface represents a common way for creation a particular Adapter.
 * As usual is implemented in separate jars.  
 *
 */
public interface MsbAdapterBuilder {
    
    void setTopic(String topic);
    
    void setMsbConfigurations(MsbConfigurations msbConfig);
    
    Adapter createAdapter();

}
