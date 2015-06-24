package com.elasticm2m.frameworks.core;

import backtype.storm.tuple.Tuple;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;

public class TerminalTupleLogger extends ElasticBaseRichBolt {

    @Override
    public void execute(Tuple tuple) {
        try {
            logger.info(tuple.getStringByField("body"));
        }
        catch(Throwable t){
            logger.error("Error logging tuple: ");
        }
        finally{
            collector.ack(tuple);
        }
    }
}
