# JSON to Avro JSON
Utility project for converting JSON to... for lack of a better way of describing it... exploded union Avro JSON. This project is a mutation of https://github.com/allegro/json-avro-converter, specifically with the ability to take raw JSON and convert it to the Union JSON expected by Avro.

Optional fields in Avro are by definition Union types, as they can be either null _or_ whatever their primitive type is, say a ```long``` or a ```string``` or an ```int``` or whatever. In order for the Avro JSON encoder to work it needs to be told explicitly what type these values are, for example your JSON might look like this:

```json
{
   "name": "Liam"
}
```

However attempting to publish this to Kafka using the Avro encoder with say... Confluent's ```kafka-avro-console-producer```... will throw an error like this:

```json
org.apache.avro.AvroTypeException: Expected start-union. Got VALUE_STRING
```

What you _actually_ need to publish looks more like this:

```json
{
   "name": {
      "string": "Liam"
   }
}
```

This presents a problem when you are just trying to stuff some JSON through Avro for testing or whatever. Anyway, this project has everything you need to stuff plain JSON in one end and get the magic Avro JSON out of the other - I could find a _lot_ of people with this issue searching online but I couldn't find any actual solutions... so when i got something working it seemed the right thing to do to share it back. 

# Usage
Usage is simple, you need an instance of the converter, your avro schema, and the json you are trying to convert.

```java
final JsonToAvroJson jsonToAvroJson = new JsonToAvroJson();

final String json = "Your JSON file";
final String schema = "Your avro schema"

final String convertedJson = jsonToAvroJson.convert(json, schema);
```