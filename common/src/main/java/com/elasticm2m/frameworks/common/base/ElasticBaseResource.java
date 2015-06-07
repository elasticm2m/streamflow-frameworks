package com.elasticm2m.frameworks.common.base;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import org.slf4j.Logger;

public abstract class ElasticBaseResource extends AbstractModule {
    
    protected Logger logger;

    @Inject
    public void setLogger(Logger logger) {
        this.logger = logger;
    }
    
    @Override
    public abstract void configure();
}
