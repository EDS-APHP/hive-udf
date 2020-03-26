package fr.aphp.wind.hive.udf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

public class UDFExtractDatePartTest {

	private UDFExtractDatePart udf;

	@Before
	public void before() {
		udf = new UDFExtractDatePart();
	}
	
	
	@Test
	public void testGePartOfDateTime() {

		DateFormat sdfParameter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		String dateStringParameter = "15/05/2019 12:41:30";

		Date d8Parameter = new Date();

		try {
			d8Parameter = sdfParameter.parse(dateStringParameter);

		} catch (ParseException e) {
			e.printStackTrace();
		} // Handle the ParseException here

		Timestamp d8ParameterTimeStamp = new Timestamp();
		d8ParameterTimeStamp.setTimeInMillis(d8Parameter.getTime());
		
		Integer actual = udf.evaluate(d8ParameterTimeStamp, "HOUR_ONLY");
		Integer expected = new Integer(12);
		Assert.assertEquals(expected, actual);
	}
}
