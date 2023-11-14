package org.jd.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.RegexValidator;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.jd.kafka.exception.TypeConversionException;
import org.jd.kafka.exception.UnsupportedTargetTypeException;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ReplaceValue<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELD_KEY = "field";
    public static final String REGEX_KEY = "regex";
    public static final String REPLACEMENT_KEY = "replacement";
    public static final String CASE_SENSITIVE_KEY = "caseSensitive";
    public static final ConfigDef config = new ConfigDef()
            .define(FIELD_KEY, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(), ConfigDef.Importance.HIGH,
                    "Field to replace. Use empty string to match whole value against regex pattern. The transformation will try to convert replaced value to target type. If such" +
                            " conversion is not supported, an exception will be thrown. E.g. replacing structures, arrays, maps, or bytes is not supported.")
            .define(REGEX_KEY, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new RegexValidator(), ConfigDef.Importance.HIGH, "Regex to match value against.")
            .define(REPLACEMENT_KEY, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(), ConfigDef.Importance.HIGH, "Replacement to use if value matches the regex. For more details os supported replacements, see documentation of <code>java.util.regex.Matcher#replaceFirst(String)</code> method.")
            .define(CASE_SENSITIVE_KEY, ConfigDef.Type.BOOLEAN, Boolean.TRUE, ConfigDef.Importance.MEDIUM, "Set this to <code>false</code> if compiled pattern should be case insensitive. Default is <code>true</code>");

    private String field;
    private boolean wholeValue;
    private Pattern regex;
    private String replacement;

    @Override
    public void configure(Map<String, ?> map) {
        SimpleConfig simpleConfig = new SimpleConfig(config, map);
        regex = Pattern.compile(simpleConfig.getString(REGEX_KEY),
                !simpleConfig.getBoolean(CASE_SENSITIVE_KEY) ? Pattern.CASE_INSENSITIVE : 0);
        replacement = simpleConfig.getString(REPLACEMENT_KEY);
        String field = simpleConfig.getString(FIELD_KEY);
        if (field.isEmpty()) {
            wholeValue = true;
            this.field = null;
        } else {
            wholeValue = false;
            this.field = field;
        }
    }

    @Override
    public ConfigDef config() {
        return config;
    }

    @Override
    public R apply(R record) {
        if (value(record) == null) {
            return record;
        }
        if (wholeValue) {
            return applyWholeValue(record);
        }
        Schema schema = schema(record);
        if (schema == null) {
            return applyNoSchema(record);
        } else {
            return applyWithSchema(record);
        }
    }

    Object replaceAndConvert(Object in) {
        if (in == null) {
            return null;
        }
        String value = in.toString();
        Matcher matcher = regex.matcher(value);
        if (matcher.matches()) {
            // replace only first
            // if there is more than one match, you can define more transformations
            return convert(in.getClass(), matcher.replaceFirst(replacement));
        }
        // return input value unchanged
        return in;
    }

    R applyWithSchema(R record) {
        Struct struct = Requirements.requireStruct(value(record), "Value replacement");
        Struct target = new Struct(schema(record));
        for (Field f : target.schema().fields()) {
            Object valueToSet;
            if (field.equals(f.name())) {
                valueToSet = replaceAndConvert(struct.get(f));
            } else {
                valueToSet = struct.get(f);
            }
            target.put(f, valueToSet);
        }
        return newRecord(record, target);
    }

    Object convert(Class<?> cls, String value) {
        try {
            if (cls == Boolean.class) {
                return Boolean.valueOf(value);
            } else if (cls == Byte.class) {
                return Byte.valueOf(value);
            } else if (cls == Short.class) {
                return Short.valueOf(value);
            } else if (cls == Integer.class) {
                return Integer.valueOf(value);
            } else if (cls == Long.class) {
                return Long.valueOf(value);
            } else if (cls == Float.class) {
                return Float.valueOf(value);
            } else if (cls == Double.class) {
                return Double.valueOf(value);
            } else if (cls == String.class) {
                return value;
            } else {
                throw new UnsupportedTargetTypeException("Unsupported target type " + cls + " for conversion.");
            }
        } catch (Exception e) {
            throw new TypeConversionException("Cannot convert '" + value + "' to target type " + cls, e);
        }
    }

    R applyNoSchema(R record) {
        Map<String, Object> data = Requirements.requireMap(value(record), "Value replacement");
        HashMap<String, Object> target = new HashMap<>();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            Object valueToSet;
            if (field.equals(entry.getKey())) {
                valueToSet = replaceAndConvert(entry.getValue());
            } else {
                valueToSet = entry.getValue();
            }
            target.put(entry.getKey(), valueToSet);
        }
        return newRecord(record, target);
    }

    R applyWholeValue(R record) {
        return newRecord(record, replaceAndConvert(value(record)));
    }

    abstract Schema schema(R record);

    abstract Object value(R record);

    abstract R newRecord(R record, Object updatedValue);

    @Override
    public void close() {
        // no-op
    }

    public static class Key<R extends ConnectRecord<R>> extends ReplaceValue<R> {

        @Override
        Schema schema(R record) {
            return record.keySchema();
        }

        @Override
        Object value(R record) {
            return record.key();
        }

        @Override
        R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends ReplaceValue<R> {

        @Override
        Schema schema(R record) {
            return record.valueSchema();
        }

        @Override
        Object value(R record) {
            return record.value();
        }

        @Override
        R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }
    }
}
