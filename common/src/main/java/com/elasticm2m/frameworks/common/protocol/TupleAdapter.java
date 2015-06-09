package com.elasticm2m.frameworks.common.protocol;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scaleset.geo.geojson.GeoJsonModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

public class TupleAdapter<T> {

    public static final Logger LOG = LoggerFactory.getLogger(TupleAdapter.class);

    private String id;

    private T body;

    private Map<String, Object> properties = new HashMap<>();

    private Class<T> typeClass;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new GeoJsonModule())
            .configure(INDENT_OUTPUT, false)
            .setSerializationInclusion(NON_EMPTY);

    public static final String ID_FIELD_NAME = "id";
    public static final String BODY_FIELD_NAME = "body";
    public static final String PROPERTIES_FIELD_NAME = "properties";


    public TupleAdapter(Class<T> typeClass) {
        this.typeClass = typeClass;
    }

    public TupleAdapter(Tuple tuple, Class<T> typeClass) {
        this.typeClass = typeClass;
        if (tuple != null && !tuple.getValues().isEmpty()) {
            loadTuple(tuple.getValues());
        }
    }

    public TupleAdapter(List<Object> tuple, Class<T> typeClass) {
        this.typeClass = typeClass;
        if (tuple != null && !tuple.isEmpty()) {
            loadTuple(tuple);
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public T getBody() {
        return body;
    }

    public void setBody(T body) {
        this.body = body;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public Object getProperty(String name) {
        Object value = null;
        if (properties != null) {
            value = properties.get(name);
        }
        return value;
    }

    public <V> V getProperty(String name, Class<V> propertyClass) {
        Object value = getProperty(name);

        if (value != null) {
            if (propertyClass.isAssignableFrom(value.getClass())) {
                return (V) value;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    public void setProperty(String name, Object value) {
        if (properties == null) {
            properties = new HashMap<>();
        }
        properties.put(name, value);
    }

    public List<Object> toTuple() throws TupleSerializationException {
        List<Object> values = new Values();
        values.add(id);
        values.add(writeValue(body));
        values.add(properties);

        return values;
    }

    public void fromTuple(Tuple tuple) {
        loadTuple(tuple.getValues());
    }

    public void fromTuple(List<Object> tuple) {
        loadTuple(tuple);
    }

    private void loadTuple(List<Object> tuple) {
        id = (String) tuple.get(0);
        body = readValue(tuple.get(1), typeClass);
        properties = (Map<String, Object>) tuple.get(2);
    }

    public <V> V convertValue(Object value, Class<V> typeClass) {
        V result = null;
        try {
            result = objectMapper.convertValue(value, typeClass);
        } catch (Exception ex) {
            LOG.error("Error converting value: ", ex);
        }
        return result;
    }

    public <V> V readValue(Object value, Class<V> typeClass) {
        V result = null;
        try {
            if (value instanceof String) {
                result = objectMapper.readValue((String) value, typeClass);
            } else if (value instanceof byte[]) {
                result = objectMapper.readValue((byte[]) value, typeClass);
            }
        } catch (Exception ex) {
            LOG.error("Error deserializing value: ", ex);
        }
        return result;
    }

    public String writeValue(Object value) {
        String result = null;
        try {
            result = objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            LOG.error("Error serializing value: ", ex);
        }
        return result;
    }

    public static Fields getFields() {
        return new Fields(ID_FIELD_NAME, BODY_FIELD_NAME, PROPERTIES_FIELD_NAME);
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();

        result.append("TupleAdapter {");
        result.append("id=").append(id).append(", ");
        result.append("properties=").append(writeValue(properties)).append(", ");
        ;
        result.append("body=").append(writeValue(body));
        result.append("}");

        return result.toString();
    }
}
