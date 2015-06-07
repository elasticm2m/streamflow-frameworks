package com.elasticm2m.frameworks.core;

import backtype.storm.utils.Utils;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichSpout;
import com.elasticm2m.frameworks.common.protocol.TupleAdapter;
import com.elasticm2m.frameworks.common.protocol.TupleSerializationException;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.Random;

public class RandomSentenceGenerator extends ElasticBaseRichSpout {

    private int sleepMillis = 100;

    Random _rand = new Random();
    String[] sentences = new String[]{"the cow jumped over the moon", "an apple a day keeps the doctor away",
            "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature"};

    @Inject(optional = true)
    public void setSleepMillis(@Named("sleep-millis") int sleepMillis) {
        this.sleepMillis = sleepMillis;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(sleepMillis);
        int rand = _rand.nextInt(sentences.length);
        String sentence = sentences[rand];
        TupleAdapter adapter = new TupleAdapter<String>(String.class);
        adapter.setBody(sentence);
        adapter.setProperty("rand", rand);
        try {
            collector.emit(adapter.toTuple());
        } catch (TupleSerializationException e) {
            logger.error("Error emitting tupple", e);
        }
    }
}
