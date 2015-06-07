package com.elasticm2m.frameworks.core;

import backtype.storm.tuple.Tuple;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;

public class PassthroughTupleLogger extends ElasticBaseRichBolt {

    @Override
    public void execute(Tuple tuple) {
        logger.info(tuple.getStringByField("body"));

        collector.emit(tuple, tuple.getValues());
        collector.ack(tuple);
    }
}
