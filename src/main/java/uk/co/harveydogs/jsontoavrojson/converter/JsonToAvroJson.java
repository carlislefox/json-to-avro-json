package uk.co.harveydogs.jsontoavrojson.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class JsonToAvroJson {

    private JsonGenericRecordReader recordReader;

    public JsonToAvroJson() {
        this.recordReader = new JsonGenericRecordReader();
    }

    /**
     * Converts the provided JSON to Avro compatible JSON
     */
    public String convert(String json, String schemaJson) throws IOException {
        final Schema schema = new Schema.Parser().parse(schemaJson);
        final GenericData.Record record = recordReader.read(json.getBytes(), schema);
        return convertGenericRecordToAvroJson(schema, record);
    }

    /**
     * Converts GenericRecord to Avro compatible JSON
     */
    private String convertGenericRecordToAvroJson(Schema schema, GenericRecord genericRecord) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, baos);

        writer.write(genericRecord, encoder);
        encoder.flush();
        baos.flush();

        return new String(baos.toByteArray());
    }

}
