package io.github.tcdl.adapters;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.github.tcdl.adapters.mock.MockAdapterFactory;
import io.github.tcdl.config.MsbConfigurations;

import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * AdapterFactoryLoaderTest.
 * @author ysavchuk
 *
 */
public class AdapterFactoryLoaderTest {

    @Test
    public void testCreatedMockAdapterByEmptyFactoryClassName(){
        String configStr = "msbConfig {}";
        Config config = ConfigFactory.parseString(configStr);
        MsbConfigurations msbConfig = new MsbConfigurations(config);
        AdapterFactoryLoader loader = new AdapterFactoryLoader(msbConfig);
        AdapterFactory adapterFactory = loader.getAdapterFactory();
        assertThat(adapterFactory, instanceOf(MockAdapterFactory.class));
    }
    
    @Test
    public void testCreatedMockAdapterByFactoryClassName(){
        String configStr = "msbConfig {brokerAdapterFactory = \"io.github.tcdl.adapters.mock.MockAdapterFactory\"}";
        Config config = ConfigFactory.parseString(configStr);
        MsbConfigurations msbConfig = new MsbConfigurations(config);
        AdapterFactoryLoader loader = new AdapterFactoryLoader(msbConfig);
        AdapterFactory adapterFactory = loader.getAdapterFactory();
        assertThat(adapterFactory, instanceOf(MockAdapterFactory.class));
    }

    @Test 
    public void testThrowExceptionByNonexistentFactoryClassName(){
        //Define Nonexistent AdapterFactory class name
        String nonexistentAdapterFactoryClassName = "io.github.tcdl.adapters.NonexistentAdapterFactory";
        String configStr = "msbConfig {brokerAdapterFactory = \"" + nonexistentAdapterFactoryClassName + "\"}";
        
        Config config = ConfigFactory.parseString(configStr);
        MsbConfigurations msbConfig = new MsbConfigurations(config);
        AdapterFactoryLoader loader = new AdapterFactoryLoader(msbConfig);
        try {
            loader.getAdapterFactory();
            fail("Created an AdapterFactory by nonexistent class!");
        } catch (RuntimeException expected) {
            assertTrue("Exception message doesn't mention" + nonexistentAdapterFactoryClassName + "'?!",
                    expected.getMessage().indexOf(nonexistentAdapterFactoryClassName) >= 0);
        }
    }
    
    @Test 
    public void testThrowExceptionByIncorrectAdapterFactoryConstructor(){
        //Define AdapterFactory class name with a class without default constructor
        String adapterFactoryClassNameWithoutDefaultConstructor = "java.lang.Integer";
        String configStr = "msbConfig {brokerAdapterFactory = \"" + adapterFactoryClassNameWithoutDefaultConstructor + "\"}";
        Config config = ConfigFactory.parseString(configStr);
        MsbConfigurations msbConfig = new MsbConfigurations(config);
        AdapterFactoryLoader loader = new AdapterFactoryLoader(msbConfig);
        try {
            loader.getAdapterFactory();
            fail("Created an AdapterFactory by class without default constructor!");
        } catch (RuntimeException expected) {
            assertTrue("Exception message doesn't mention" + adapterFactoryClassNameWithoutDefaultConstructor + "'?!",
                    expected.getMessage().indexOf(adapterFactoryClassNameWithoutDefaultConstructor) >= 0);
        }
    }

    @Test 
    public void testThrowExceptionByIncorrectAdapterFactoryInterfaceImplementation(){
        //Define AdapterFactory class name with a class that doesn't implement AdapterFactory interface
        String incorrectAdapterFactoryImplementationClassName = "java.leng.StringBuilder";
        String configStr = "msbConfig {brokerAdapterFactory = \"" + incorrectAdapterFactoryImplementationClassName + "\"}";
        Config config = ConfigFactory.parseString(configStr);
        MsbConfigurations msbConfig = new MsbConfigurations(config);
        AdapterFactoryLoader loader = new AdapterFactoryLoader(msbConfig);
        try {
            loader.getAdapterFactory();
            fail("Created an AdapterFactory by class that doesn't implement AdapterFActory interface!");
        } catch (RuntimeException expected) {
            assertTrue("Exception message doesn't mention" + incorrectAdapterFactoryImplementationClassName + "'?!",
                    expected.getMessage().indexOf(incorrectAdapterFactoryImplementationClassName) >= 0);
        }
    }
    

}