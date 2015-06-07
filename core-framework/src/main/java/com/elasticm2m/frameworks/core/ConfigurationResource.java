package com.elasticm2m.frameworks.core;

import com.elasticm2m.frameworks.common.base.ElasticBaseResource;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ConfigurationResource extends ElasticBaseResource {

    private String configData = "";

    @Inject(optional = true)
    public void setConfigData(@Named("config-data") String configData) {
        this.configData = configData;
    }

    @Override
    public void configure() {
        Config config = ConfigFactory.parseString(configData);
        bind(Config.class).toInstance(config);
    }
}
