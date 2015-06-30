package com.elasticm2m.frameworks.core;

import backtype.storm.tuple.Tuple;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;

public class PassthroughTupleLogger extends ElasticBaseRichBolt {

    @Override
    public void execute(Tuple tuple) {
        try{
            logger.info(tuple.getStringByField("body"));

            collector.emit(tuple, tuple.getValues());
        }
        catch(Throwable t){
            logger.error("Error logging {}", t);
        }
        finally {
            collector.ack(tuple);
        }
    }
}
