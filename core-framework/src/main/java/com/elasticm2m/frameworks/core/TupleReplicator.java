package com.elasticm2m.frameworks.core;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.elasticm2m.frameworks.common.protocol.TupleAdapter;

public class TupleReplicator extends ElasticBaseRichBolt {

    @Override
    public void execute(Tuple tuple) {
        // Replicate the input tuple data accross multiple streams
        collector.emit("primary", tuple, tuple.getValues());
        collector.emit("secondary", tuple, tuple.getValues());
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("primary", TupleAdapter.getFields());
        declarer.declareStream("secondary", TupleAdapter.getFields());
    }
}
