package com.elasticm2m.frameworks.core;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;

public class Noop extends ElasticBaseRichBolt {

    @Override
    public void execute(Tuple tuple) {
        logger.debug("Noop consuming tuple");
        collector.ack(tuple);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}
