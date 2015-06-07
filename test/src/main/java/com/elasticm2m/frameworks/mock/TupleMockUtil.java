package com.elasticm2m.frameworks.mock;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;
import com.elasticm2m.frameworks.common.protocol.TupleAdapter;
import com.elasticm2m.frameworks.common.protocol.TupleSerializationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TupleMockUtil {

    public static Tuple mockTickTuple() {
        return mockTuple(Constants.SYSTEM_COMPONENT_ID, Constants.SYSTEM_TICK_STREAM_ID);
    }

    public static Tuple mockTuple(String componentId, String streamId) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn(componentId);
        when(tuple.getSourceStreamId()).thenReturn(streamId);
        return tuple;
    }

    public static Tuple mockGenericTuple(String componentId, String streamId, List<Object> tupleValues) {
        Tuple tuple = mockTuple(componentId, streamId);
        when(tuple.getValues()).thenReturn(tupleValues);
        for (int i = 0; i < tupleValues.size(); i++) {
            when(tuple.getValue(i)).thenReturn(tupleValues.get(i));
        }
        return tuple;
    }
    
    public static Tuple mockAdapterTuple(String componentId, String streamId, TupleAdapter tupleAdapter) throws TupleSerializationException {
        List<Object> values = tupleAdapter.toTuple();
        
        Map<String, Object> fields = new HashMap<>();
        fields.put(TupleAdapter.ID_FIELD_NAME, values.get(0));
        fields.put(TupleAdapter.BODY_FIELD_NAME, values.get(1));
        fields.put(TupleAdapter.PROPERTIES_FIELD_NAME, values.get(2));
        
        Tuple tuple = mockTupleByField(componentId, streamId, fields);
        when(tuple.getValues()).thenReturn(values);
        return tuple;
    }
    
    public static Tuple mockTupleByField(String componentId, String streamId, Map<String, Object> fields) {
        Tuple tuple = mockTuple(componentId, streamId);
        for (Entry<String, Object> field : fields.entrySet()) {
            when(tuple.getValueByField(field.getKey())).thenReturn(field.getValue());
            if (field.getValue() instanceof String) {
                when(tuple.getStringByField(field.getKey())).thenReturn((String) field.getValue());
            }
            
            // TODO: ADD TYPE CHECKS FOR ALL PRIMITIVE WRAPPERS
        }
        return tuple;
    }
}
