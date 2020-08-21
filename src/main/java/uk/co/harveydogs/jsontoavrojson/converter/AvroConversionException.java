package uk.co.harveydogs.jsontoavrojson.converter;

import org.apache.avro.AvroRuntimeException;

public class AvroConversionException extends AvroRuntimeException {

    public AvroConversionException(String message, Throwable cause) {
        super(message, cause);
    }

}
