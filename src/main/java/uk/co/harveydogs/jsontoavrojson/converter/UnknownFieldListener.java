package uk.co.harveydogs.jsontoavrojson.converter;

public interface UnknownFieldListener {

	void onUnknownField(String name, Object value, String path);

}
