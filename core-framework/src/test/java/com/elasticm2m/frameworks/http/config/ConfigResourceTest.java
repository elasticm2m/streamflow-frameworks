package com.elasticm2m.frameworks.http.config;

import com.elasticm2m.frameworks.http.ConfigurationResource;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigResourceTest extends Assert {

    @Test
    public void testLoadConfig() {
        Injector resourceInjector = Guice.createInjector(new TestModule());
        ConfigurationResource configurationResource = resourceInjector.getInstance(ConfigurationResource.class);
        Injector injector = Guice.createInjector(configurationResource);
        Config config = injector.getInstance(Config.class);
        assertNotNull(config);
        assertEquals("y", config.getString("x"));
    }

    static class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(String.class).annotatedWith(Names.named("config-data")).toInstance("x: y");
            bind(Logger.class).toInstance(LoggerFactory.getLogger(getClass()));
        }
    }
}
