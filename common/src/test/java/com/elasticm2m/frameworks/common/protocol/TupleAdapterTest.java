package com.elasticm2m.frameworks.common.protocol;

import com.elasticm2m.sdk.repository.model.Event;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

public class TupleAdapterTest {

    @Test
    public void adapterToTuple() throws Exception {
        Event inEvent = new Event();
        inEvent.setId("test-device");
        inEvent.setDeviceId("device-id");
        inEvent.setDeviceKey("device-key");
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
        assertEquals(inEvent.getDeviceId(), outEvent.getDeviceId());
        assertEquals(inEvent.getDeviceKey(), outEvent.getDeviceKey());
        assertEquals(inEvent.getTime(), outEvent.getTime());
        assertEquals(1, outProperties.get("integer-property"));
        assertEquals("this is a string", outProperties.get("string-property"));
    }

    @Test
    public void tupleToAdapter() {
        List<Object> fields = new ArrayList<>();
        fields.add("sample-id");
        fields.add("{\"deviceId\":\"device-id\",\"deviceKey\":\"device-key\",\"time\":1000,\"_id\":\"test-device\"}");
        fields.add(null);
        
        TupleAdapter<Event> wrapper = new TupleAdapter<>(Event.class);
        wrapper.fromTuple(fields);
        
        Event event = wrapper.getBody();
        
        // Verify bound event values
        assertEquals("test-device", event.getId());
        assertEquals("device-id", event.getDeviceId());
        assertEquals("device-key", event.getDeviceKey());
        assertEquals(1000, event.getTime());
    }
}
