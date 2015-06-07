package com.elasticm2m.frameworks.common.protocol;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TupleAdapterTest {

    @Test
    public void adapterToTuple() throws Exception {
        Event inEvent = new Event();
        inEvent.setId("test-device");
        inEvent.setTime(System.currentTimeMillis());

        TupleAdapter<Event> wrapper = new TupleAdapter<>(Event.class);
        wrapper.setId("custom-tuple");
        wrapper.setBody(inEvent);
        wrapper.setProperty("integer-property", 1);
        wrapper.setProperty("string-property", "this is a string");

        List<Object> values = wrapper.toTuple();

        String outId = (String) values.get(0);
        Event outEvent = wrapper.readValue((String) values.get(1), Event.class);
        Map<String, Object> outProperties = (Map) values.get(2);

        assertEquals("custom-tuple", outId);
        assertEquals(inEvent.getId(), outEvent.getId());
        assertEquals(inEvent.getTime(), outEvent.getTime());
        assertEquals(1, outProperties.get("integer-property"));
        assertEquals("this is a string", outProperties.get("string-property"));
    }

    @Test
    public void tupleToAdapter() {
        List<Object> fields = new ArrayList<>();
        fields.add("sample-id");
        fields.add("{\"time\":1000,\"id\":\"test-device\"}");
        fields.add(null);

        TupleAdapter<Event> wrapper = new TupleAdapter<>(Event.class);
        wrapper.fromTuple(fields);

        Event event = wrapper.getBody();

        // Verify bound event values
        assertEquals("test-device", event.getId());
        assertEquals(1000, event.getTime());
    }

    static class Event {

        private String id;
        private long time;

        public String getId() {
            return id;
        }

        public long getTime() {
            return time;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setTime(long time) {
            this.time = time;
        }
    }
}
