package com.elasticm2m.frameworks.common.base;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.elasticm2m.frameworks.common.protocol.TupleAdapter;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.typesafe.config.Config;
import java.util.Map;
import org.slf4j.Logger;

public abstract class ElasticBaseRichSpout extends BaseRichSpout {
    
    protected Map stormConf;
    
    protected TopologyContext topologyContext;
    
    protected SpoutOutputCollector collector;
    
    protected String componentName;
    
    protected String componentLabel;

    protected Logger logger;

    protected Config config;
    
    @Inject(optional = true)
    public void setConfig(Config config) {
        this.config = config;
    }

    @Inject
    public void setLogger(Logger logger) {
        this.logger = logger;
    }
    
    @Inject(optional=true)
    public void setComponentName(@Named("streamflow.component.name") String componentName) {
        this.componentName = componentName;
    }
    
    @Inject(optional=true)
    public void setComponentLabel(@Named("streamflow.component.label") String componentLabel) {
        this.componentLabel = componentLabel;
    }

    @Override
    public void open(Map stormConf, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.stormConf = stormConf;
        this.topologyContext = topologyContext;
        this.collector = collector;
        
        logger.info(componentLabel + " started");
    }

    @Override
    public abstract void nextTuple();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(TupleAdapter.getFields());
    }
}
