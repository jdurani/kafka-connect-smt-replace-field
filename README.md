# Replace value Kafka Connect SMT

This is an SMT plugin for Kafka Connect.
It can modify Kafka message value against regex.

# Supported scenarios

## Complex records

The plugin can convert value of desired field of complex record. The target field must be in a root. I.e. nested fields are not supported at the moment.
However, record can contain nested objects.

## Simple records

Simple fields are also supported. E.g. if value is supported primitive type, you can replace whole value.

## Supported types

The plugin matches regex against string representation of a value. It means, it is capable of matching/replacing also numbers and booleans.
However, plugin does not change the schema. Therefor it must convert replaced field back to original type.
Target value is converted using respective method `valueOf(Strin)`.

Supported types:

* Boolean
* Byte
* Short
* Integer
* Long
* Float
* Double
* String

# Installation

Plugin is installed as standard Kafka connect plugins. It requires no other libraries but standard Kafka connect libraries and plugin JAR.

# Configuration

| Property name   | Type    | Default value | Description                                                                                                 |
|-----------------|---------|---------------|-------------------------------------------------------------------------------------------------------------|
| `field`         | String  | -             | Field to replace value. Use empty string to match whole value against regex pattern.                        |
| `regex`         | Pattern | -             | Regex to match value. This is standard Java Pattern.                                                        |
| `replacement`   | String  | -             | Replacement string. For more details of supported format see `java.util.regex.Matcher#replaceFirst(String)` |
| `caseSensitive` | Boolean | `true`        | Set this to `false` if compiled pattern should be case insensitive.                                         |
