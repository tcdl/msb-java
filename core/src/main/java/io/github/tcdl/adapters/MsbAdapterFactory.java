package io.github.tcdl.adapters;

import io.github.tcdl.config.MsbConfigurations;

/**
 * MSBAdapterFactory abstract class represents a common way for creation a particular AdapterFactory
 * accordingly to MSB Configuration.
 */

public abstract class MsbAdapterFactory {
    
    public MsbAdapterFactory(MsbConfigurations msbConfig) {
        //No-op
    }

    public abstract Adapter createAdapter(String topic);

}
