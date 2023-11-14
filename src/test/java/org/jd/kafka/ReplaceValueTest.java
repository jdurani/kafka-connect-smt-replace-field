package org.jd.kafka;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jd.kafka.exception.TypeConversionException;
import org.jd.kafka.exception.UnsupportedTargetTypeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Set;

class ReplaceValueTest {

    private ReplaceValue<SinkRecord> test;

    @BeforeEach
    void beforeEach() {
        configureSpy(Map.of(ReplaceValue.FIELD_KEY, "fld", ReplaceValue.REPLACEMENT_KEY, "target", ReplaceValue.REGEX_KEY, "^source$"));
    }

    @SuppressWarnings("unchecked")
    private void configureSpy(Map<String, ?> map) {
        test = Mockito.mock(ReplaceValue.class, Answers.CALLS_REAL_METHODS);
        test.configure(map);
        // to not have to check this in test methods if we want to check no more interactions
        Mockito.verify(test).configure(Mockito.any());
    }

    @Test
    void applyTombstone() {
        SinkRecord sr = Mockito.mock(SinkRecord.class);
        Mockito.doReturn(null).when(test).value(Mockito.same(sr));
        Assertions.assertSame(sr, test.apply(sr));
        Mockito.verify(test).apply(Mockito.any());
        Mockito.verify(test).value(Mockito.any());
        Mockito.verifyNoMoreInteractions(test);
    }

    @Test
    void applyWithSchema() {
        SinkRecord sr = Mockito.mock(SinkRecord.class);
        Mockito.doReturn(new Object()).when(test).value(Mockito.same(sr));
        Mockito.doReturn(null).when(test).applyWholeValue(Mockito.any());
        Mockito.doReturn(null).when(test).applyNoSchema(Mockito.any());
        SinkRecord toBeReturned = Mockito.mock(SinkRecord.class);
        Mockito.doReturn(toBeReturned).when(test).applyWithSchema(Mockito.any());
        Mockito.doReturn(Schema.BOOLEAN_SCHEMA).when(test).schema(Mockito.same(sr));

        Assertions.assertSame(toBeReturned, test.apply(sr));
    }

    @Test
    void applyNoSchema() {
        SinkRecord sr = Mockito.mock(SinkRecord.class);
        Mockito.doReturn(new Object()).when(test).value(Mockito.same(sr));
        Mockito.doReturn(null).when(test).applyWholeValue(Mockito.any());
        Mockito.doReturn(null).when(test).applyWithSchema(Mockito.any());
        SinkRecord toBeReturned = Mockito.mock(SinkRecord.class);
        Mockito.doReturn(toBeReturned).when(test).applyNoSchema(Mockito.any());
        Mockito.doReturn(null).when(test).schema(Mockito.same(sr));

        Assertions.assertSame(toBeReturned, test.apply(sr));
    }

    @Test
    void applyWholeValue() {
        // prepare with empty field
        configureSpy(Map.of(ReplaceValue.FIELD_KEY, "", ReplaceValue.REPLACEMENT_KEY, "target", ReplaceValue.REGEX_KEY, "^source$"));
        SinkRecord sr = Mockito.mock(SinkRecord.class);
        Mockito.doReturn(new Object()).when(test).value(Mockito.same(sr));
        Mockito.doReturn(null).when(test).applyWithSchema(Mockito.any());
        Mockito.doReturn(null).when(test).applyNoSchema(Mockito.any());
        SinkRecord toBeReturned = Mockito.mock(SinkRecord.class);
        Mockito.doReturn(toBeReturned).when(test).applyWholeValue(Mockito.any());

        Mockito.doReturn(null).when(test).schema(Mockito.same(sr));

        Assertions.assertSame(toBeReturned, test.apply(sr));
    }

    @Test
    void replaceAndConvertNull() {
        Assertions.assertNull(test.replaceAndConvert(null));
    }

    @Test
    void replaceAndConvertNoMatch() {
        String in = "xxx";
        Assertions.assertSame(in, test.replaceAndConvert(in));
        Mockito.verify(test, Mockito.times(0)).convert(Mockito.any(), Mockito.any());
    }

    @Test
    void replaceAndConvertMatch() {
        // 'source' replaced with 'target' then converted to 'converted' - this is to test, that we call convert properly
        Mockito.doReturn("converted").when(test).convert(Mockito.same(String.class), Mockito.eq("target"));
        Assertions.assertEquals("converted", test.replaceAndConvert("source"));
    }

    @Test
    void applyWithSchemaMethod() {
        SinkRecord in = Mockito.mock(SinkRecord.class);
        SinkRecord out = Mockito.mock(SinkRecord.class);
        Mockito.doReturn("replaced").when(test).replaceAndConvert(Mockito.eq("src"));
        Schema schema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .field("b", Schema.BOOLEAN_SCHEMA)
                .field("fld", Schema.STRING_SCHEMA)
                .build();
        Struct inStruct = new Struct(schema);
        inStruct.put("a", 1);
        inStruct.put("b", Boolean.TRUE);
        inStruct.put("fld", "src");
        Mockito.doReturn(inStruct).when(test).value(Mockito.same(in));
        Mockito.doReturn(schema).when(test).schema(Mockito.same(in));

        Mockito.doReturn(out).when(test).newRecord(Mockito.same(in),
                Mockito.<Struct>argThat(s -> s.schema() == schema // pointer equality is intention
                        && s.getInt32("a").equals(1)
                        && s.getBoolean("b").equals(Boolean.TRUE)
                        && s.getString("fld").equals("replaced")));

        // arguments are checked in mocking
        Assertions.assertSame(out, test.applyWithSchema(in));
    }

    @Test
    void applyNoSchemaMethod() {
        SinkRecord in = Mockito.mock(SinkRecord.class);
        SinkRecord out = Mockito.mock(SinkRecord.class);
        Mockito.doReturn("replaced").when(test).replaceAndConvert(Mockito.eq("src"));
        Map<String, Object> inMap = Map.of("a", 1, "b", Boolean.TRUE, "fld", "src");
        Mockito.doReturn(inMap).when(test).value(Mockito.same(in));
        Mockito.doReturn(null).when(test).schema(Mockito.same(in));

        Mockito.doReturn(out).when(test).newRecord(Mockito.same(in),
                Mockito.<Map<String, Object>>argThat(m -> m.size() == 3
                        && m.keySet().equals(Set.of("a", "b", "fld"))
                        && m.get("a").equals(1)
                        && m.get("b").equals(Boolean.TRUE)
                        && m.get("fld").equals("replaced")));

        // arguments are checked in mocking
        Assertions.assertSame(out, test.applyNoSchema(in));
    }

    @Test
    void applyWholeValueMethod() {
        SinkRecord sr = Mockito.mock(SinkRecord.class);
        Object value = new Object();
        Object replaced = new Object();
        Mockito.doReturn(value).when(test).value(Mockito.same(sr));
        Mockito.doReturn(replaced).when(test).replaceAndConvert(Mockito.same(value));
        SinkRecord outRec = Mockito.mock(SinkRecord.class);
        Mockito.doReturn(outRec).when(test).newRecord(Mockito.same(sr), Mockito.same(replaced));

        Assertions.assertSame(outRec, test.applyWholeValue(sr));
    }

    @Test
    void convert() {
        Assertions.assertAll(
                () -> Assertions.assertEquals(Boolean.TRUE, test.convert(Boolean.class, "true")),
                () -> Assertions.assertEquals(Boolean.FALSE, test.convert(Boolean.class, "false")),
                () -> Assertions.assertEquals((byte) 10, test.convert(Byte.class, "10")),
                () -> Assertions.assertEquals((byte) -10, test.convert(Byte.class, "-10")),
                () -> Assertions.assertEquals((short) 20, test.convert(Short.class, "20")),
                () -> Assertions.assertEquals((short) -20, test.convert(Short.class, "-20")),
                () -> Assertions.assertEquals(30, test.convert(Integer.class, "30")),
                () -> Assertions.assertEquals(-30, test.convert(Integer.class, "-30")),
                () -> Assertions.assertEquals(40L, test.convert(Long.class, "40")),
                () -> Assertions.assertEquals(-40L, test.convert(Long.class, "-40")),
                () -> Assertions.assertEquals(12.23f, test.convert(Float.class, "12.23")),
                () -> Assertions.assertEquals(-12.23f, test.convert(Float.class, "-12.23")),
                () -> Assertions.assertEquals(65.54, test.convert(Double.class, "65.54")),
                () -> Assertions.assertEquals(-65.54, test.convert(Double.class, "-65.54")),
                () -> Assertions.assertEquals("abc", test.convert(String.class, "abc")));
    }

    @Test
    void convertUnknownClass() {
        Throwable cause = Assertions.assertThrowsExactly(TypeConversionException.class, () -> test.convert(Object.class, null)).getCause();
        Assertions.assertNotNull(cause);
        Assertions.assertEquals(UnsupportedTargetTypeException.class, cause.getClass());
    }

    @Test
    void convertConversionFail() {
        Throwable cause = Assertions.assertThrowsExactly(TypeConversionException.class, () -> test.convert(Byte.class, "a")).getCause();
        Assertions.assertNotNull(cause);
        Assertions.assertEquals(NumberFormatException.class, cause.getClass());
    }

    @Test
    void keySchema() {
        SinkRecord sr = Mockito.mock(SinkRecord.class);
        Schema wanted = Mockito.mock(Schema.class);
        Mockito.doReturn(wanted).when(sr).keySchema();

        Assertions.assertSame(wanted, new ReplaceValue.Key<SinkRecord>().schema(sr));
    }

    @Test
    void keyValue() {
        SinkRecord sr = Mockito.mock(SinkRecord.class);
        Object wanted = new Object();
        Mockito.doReturn(wanted).when(sr).key();

        Assertions.assertSame(wanted, new ReplaceValue.Key<SinkRecord>().value(sr));
    }

    @Test
    void keyNewRecord() {
        SinkRecord sr = new SinkRecord("topic", 10, Schema.INT32_SCHEMA, "key", Schema.BOOLEAN_SCHEMA, "value", 1000L, 100L, TimestampType.CREATE_TIME);

        SinkRecord out = new ReplaceValue.Key<SinkRecord>().newRecord(sr, "replaced");

        Assertions.assertAll(
                () -> Assertions.assertEquals("topic", out.topic()),
                () -> Assertions.assertEquals(10, out.kafkaPartition()),
                () -> Assertions.assertEquals("replaced", out.key()),
                () -> Assertions.assertEquals(Schema.INT32_SCHEMA, out.keySchema()),
                () -> Assertions.assertEquals("value", out.value()),
                () -> Assertions.assertEquals(Schema.BOOLEAN_SCHEMA, out.valueSchema()),
                () -> Assertions.assertEquals(100L, out.timestamp()));
    }

    @Test
    void valueSchema() {
        SinkRecord sr = Mockito.mock(SinkRecord.class);
        Schema wanted = Mockito.mock(Schema.class);
        Mockito.doReturn(wanted).when(sr).valueSchema();

        Assertions.assertSame(wanted, new ReplaceValue.Value<SinkRecord>().schema(sr));
    }

    @Test
    void valueValue() {
        SinkRecord sr = Mockito.mock(SinkRecord.class);
        Object wanted = new Object();
        Mockito.doReturn(wanted).when(sr).value();

        Assertions.assertSame(wanted, new ReplaceValue.Value<SinkRecord>().value(sr));
    }

    @Test
    void valueNewRecord() {
        SinkRecord sr = new SinkRecord("topic", 10, Schema.INT32_SCHEMA, "key", Schema.BOOLEAN_SCHEMA, "value", 1000L, 100L, TimestampType.CREATE_TIME);

        SinkRecord out = new ReplaceValue.Value<SinkRecord>().newRecord(sr, "replaced");

        Assertions.assertAll(
                () -> Assertions.assertEquals("topic", out.topic()),
                () -> Assertions.assertEquals(10, out.kafkaPartition()),
                () -> Assertions.assertEquals("key", out.key()),
                () -> Assertions.assertEquals(Schema.INT32_SCHEMA, out.keySchema()),
                () -> Assertions.assertEquals("replaced", out.value()),
                () -> Assertions.assertEquals(Schema.BOOLEAN_SCHEMA, out.valueSchema()),
                () -> Assertions.assertEquals(100L, out.timestamp()));
    }
}
