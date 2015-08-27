package com.elasticm2m.frameworks.groovy;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import groovy.lang.GroovyShell;
import groovy.lang.Script;

import java.util.List;
import java.util.Map;

public class GroovyTransformBolt extends ElasticBaseRichBolt {

    private String script;
    private Script compiledScript;

    @Inject
    public void setScript(@Named("script") String script) {
        this.script = script;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
        try {
            super.prepare(conf, topologyContext, collector);
            GroovyShell shell = new GroovyShell();
            compiledScript = shell.parse(script);
        } catch (Throwable e) {
            logger.error("Unable to prepare service", e);
            throw e;
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Object body = tuple.getValue(1);
            compiledScript.setProperty("body", body);
            compiledScript.setProperty("properties", tuple.getValue(2));
            Object result = compiledScript.run();
            List<Object> values = new Values(tuple.getValue(0), result, tuple.getValue(2));
            collector.emit(tuple, values);
            collector.ack(tuple);
        } catch (Throwable e) {
            logger.error("Unable to process tuple", e);
            collector.fail(tuple);
        }
    }

}