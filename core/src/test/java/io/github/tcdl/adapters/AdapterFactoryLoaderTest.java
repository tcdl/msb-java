package io.github.tcdl.adapters;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.github.tcdl.adapters.mock.MockAdapterFactory;
import io.github.tcdl.config.MsbConfig;

import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * AdapterFactoryLoaderTest.
 * @author ysavchuk
 *
 */
public class AdapterFactoryLoaderTest {

    private String basicConfigWithoutAdapterFactory = "msbConfig { %s timerThreadPoolSize = 1, validateMessage = true, "
            + " serviceDetails = {name = \"test_msb\", version = \"1.0.1\", instanceId = \"msbd06a-ed59-4a39-9f95-811c5fb6ab87\"} }";
    
    @Test
    public void testCreatedMockAdapterByFactoryClassName(){
        String configStr = String.format(basicConfigWithoutAdapterFactory, "brokerAdapterFactory = \"io.github.tcdl.adapters.mock.MockAdapterFactory\",");
        Config config = ConfigFactory.parseString(configStr);
        MsbConfig msbConfig = new MsbConfig(config);
        AdapterFactoryLoader loader = new AdapterFactoryLoader(msbConfig);
        AdapterFactory adapterFactory = loader.getAdapterFactory();
        assertThat(adapterFactory, instanceOf(MockAdapterFactory.class));
    }

    @Test 
    public void testThrowExceptionByNonexistentFactoryClassName(){
        //Define Nonexistent AdapterFactory class name
        String nonexistentAdapterFactoryClassName = "io.github.tcdl.adapters.NonexistentAdapterFactory";
        String configStr = String.format(basicConfigWithoutAdapterFactory, "brokerAdapterFactory = \"" + nonexistentAdapterFactoryClassName + "\", ");
                Config config = ConfigFactory.parseString(configStr);
        MsbConfig msbConfig = new MsbConfig(config);
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
        String configStr = String.format(basicConfigWithoutAdapterFactory, "brokerAdapterFactory = \"" + adapterFactoryClassNameWithoutDefaultConstructor + "\", ");
        Config config = ConfigFactory.parseString(configStr);
        MsbConfig msbConfig = new MsbConfig(config);
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
        String incorrectAdapterFactoryImplementationClassName = "java.lang.StringBuilder";
        String configStr = String.format(basicConfigWithoutAdapterFactory, "brokerAdapterFactory = \"" + incorrectAdapterFactoryImplementationClassName + "\", ");
        Config config = ConfigFactory.parseString(configStr);
        MsbConfig msbConfig = new MsbConfig(config);
        AdapterFactoryLoader loader = new AdapterFactoryLoader(msbConfig);
        try {
            loader.getAdapterFactory();
            fail("Created an AdapterFactory by class that doesn't implement AdapterFactory interface!");
        } catch (RuntimeException expected) {
            assertTrue("Exception message doesn't mention" + incorrectAdapterFactoryImplementationClassName + "'?!",
                    expected.getMessage().indexOf(incorrectAdapterFactoryImplementationClassName) >= 0);
        }
    }
    

}