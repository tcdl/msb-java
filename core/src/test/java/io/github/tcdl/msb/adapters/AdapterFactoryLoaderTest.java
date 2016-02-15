package io.github.tcdl.msb.adapters;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.github.tcdl.msb.config.MsbConfig;

import io.github.tcdl.msb.mock.adapterfactory.TestMsbAdapterFactory;
import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AdapterFactoryLoaderTest {

    private static final Config CONFIG = ConfigFactory.load("reference.conf");

    private MsbConfig msbConfigSpy;

    @Before
    public void setUp() {
        msbConfigSpy = spy(new MsbConfig(CONFIG));
    }

    @Test
    public void testCreatedMockAdapterByFactoryClassName(){
        when(msbConfigSpy.getBrokerAdapterFactory()).thenReturn("io.github.tcdl.msb.mock.adapterfactory.TestMsbAdapterFactory");
        AdapterFactoryLoader loader = new AdapterFactoryLoader(msbConfigSpy);
        AdapterFactory adapterFactory = loader.getAdapterFactory();
        assertThat(adapterFactory, instanceOf(TestMsbAdapterFactory.class));
    }

    @Test 
    public void testThrowExceptionByNonexistentFactoryClassName(){
        //Define Nonexistent AdapterFactory class name
        String nonexistentAdapterFactoryClassName = "io.github.tcdl.msb.adapters.NonexistentAdapterFactory";
        when(msbConfigSpy.getBrokerAdapterFactory()).thenReturn(nonexistentAdapterFactoryClassName);
        AdapterFactoryLoader loader = new AdapterFactoryLoader(msbConfigSpy);
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
        when(msbConfigSpy.getBrokerAdapterFactory()).thenReturn(adapterFactoryClassNameWithoutDefaultConstructor);
        AdapterFactoryLoader loader = new AdapterFactoryLoader(msbConfigSpy);
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
        when(msbConfigSpy.getBrokerAdapterFactory()).thenReturn(incorrectAdapterFactoryImplementationClassName);
        AdapterFactoryLoader loader = new AdapterFactoryLoader(msbConfigSpy);
        try {
            loader.getAdapterFactory();
            fail("Created an AdapterFactory by class that doesn't implement AdapterFactory interface!");
        } catch (RuntimeException expected) {
            assertTrue("Exception message doesn't mention" + incorrectAdapterFactoryImplementationClassName + "'?!",
                    expected.getMessage().indexOf(incorrectAdapterFactoryImplementationClassName) >= 0);
        }
    }

}