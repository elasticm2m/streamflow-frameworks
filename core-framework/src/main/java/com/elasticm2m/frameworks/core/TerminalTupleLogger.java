package com.elasticm2m.frameworks.core;

import backtype.storm.tuple.Tuple;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;

public class TerminalTupleLogger extends ElasticBaseRichBolt {

    @Override
    public void execute(Tuple tuple) {
        logger.info(tuple.getStringByField("body"));
        collector.ack(tuple);
    }
}
