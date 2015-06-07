package com.elasticm2m.frameworks.common.base;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.elasticm2m.frameworks.common.protocol.TupleAdapter;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import java.util.Map;

public abstract class ElasticBaseRichBolt extends BaseRichBolt {
    
    protected Map stormConf;
    
    protected TopologyContext topologyContext;

    protected OutputCollector collector;
    
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
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector collector) {
        this.stormConf = stormConf;
        this.topologyContext = topologyContext;
        this.collector = collector;
        
        logger.info(componentLabel + " started");
    }

    @Override
    abstract public void execute(Tuple tuple);
 
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(TupleAdapter.getFields());
    }
}
