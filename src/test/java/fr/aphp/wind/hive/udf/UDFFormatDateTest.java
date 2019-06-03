package fr.aphp.wind.hive.udf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

public class UDFFormatDateTest {

	private UDFFormatDate udf;

	@Before
	public void before() {
		udf = new UDFFormatDate();
	}
	
	
	@Test
	public void testGetDateFromDateTime() {

		DateFormat sdfParameter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		DateFormat sdfExpected = new SimpleDateFormat("dd/MM/yyyy");

		String dateStringParameter = "15/05/2019 12:41:30";
		String dateStringExpected = "15/05/2019";

		Date d8Parameter = new Date();
		Date d8Expected = new Date();

		try {
			d8Parameter = sdfParameter.parse(dateStringParameter);
			d8Expected = sdfExpected.parse(dateStringExpected);

		} catch (ParseException e) {
			e.printStackTrace();
		} // Handle the ParseException here

		Date actual = udf.evaluate(d8Parameter);
		Assert.assertEquals(d8Expected, actual);

	}

}
