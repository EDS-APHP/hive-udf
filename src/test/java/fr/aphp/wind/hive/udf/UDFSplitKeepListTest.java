package fr.aphp.wind.hive.udf;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

public class UDFSplitKeepListTest {

	private UDFSplitKeepList udf;

	@Before
	public void before() {
		udf = new UDFSplitKeepList();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testEvaluate() throws UDFArgumentException {
		String t = "Hémoglobine 10.450 g/dL faible taux";
		ArrayList<String> listRegex = new ArrayList<String>();

		listRegex.add("(\\d*\\.\\d*)");
		listRegex.add("(g/dL)");

		UDFSplitKeepList udf = new fr.aphp.wind.hive.udf.UDFSplitKeepList();
		LinkedList<String> results = udf.evaluate(listRegex, t);

		LinkedList<String> result = new LinkedList<String>();
		// regex 1
		
		result.add("Hémoglobine");
		result.add("10.450");
		result.add("g/dL faible taux");
		// regex 2 
		result.add("Hémoglobine 10.450");
		result.add("g/dL");
		result.add("faible taux");

		Assert.assertEquals(result, results);
	}
	
	
	//@Test(expected = UDFArgumentException.class)
	public void testEvaluate_rightBracketMissing() throws UDFArgumentException {
		String t = "Hémoglobine 10.450 g/dL faible taux";
		ArrayList<String> listRegex = new ArrayList<String>();

		listRegex.add("(\\d*\\.\\d*");
		
		UDFSplitKeepList udf = new fr.aphp.wind.hive.udf.UDFSplitKeepList();
		//LinkedList<String> results = udf.evaluate(listRegex, t);
		udf.evaluate(listRegex, t);
	}
	
	
	//@Test(expected = UDFArgumentException.class)
	public void testEvaluate_multi_groups() throws UDFArgumentException {
		String t = "Hémoglobine 10.450 g/dL faible taux";
		ArrayList<String> listRegex = new ArrayList<String>();

		listRegex.add("(\\d*\\.\\d* (Cats))");
		
		UDFSplitKeepList udf = new fr.aphp.wind.hive.udf.UDFSplitKeepList();
		//LinkedList<String> results = udf.evaluate(listRegex, t);
		udf.evaluate(listRegex, t);
	}
	

	@Test
	public void testIsValidGroupRegexOK() {

		String regexIn = "(123[a-b]*)";

		UDFSplitKeepList udf = new fr.aphp.wind.hive.udf.UDFSplitKeepList();
		assertTrue(udf.isValidRegexGroup(regexIn));
	}
	
	@Test
	public void testIsValidGroupRegexOK_doubleBracket() {

		String regexIn = "((123[a-b]*)";

		UDFSplitKeepList udf = new fr.aphp.wind.hive.udf.UDFSplitKeepList();
		assertTrue(udf.isValidRegexGroup(regexIn));
	}
	
	@Test
	public void testIsValidGroupRegexOK_empty() {

		String regexIn = "()";

		UDFSplitKeepList udf = new fr.aphp.wind.hive.udf.UDFSplitKeepList();
		assertTrue(udf.isValidRegexGroup(regexIn));
	}

	@Test
	public void testIsValidGroupRegexKO_rightBracketMissing() {

		String regexIn = "(123[a-b]*";

		UDFSplitKeepList udf = new fr.aphp.wind.hive.udf.UDFSplitKeepList();
		assertFalse(udf.isValidRegexGroup(regexIn));
	}

	@Test
	public void testIsValidGroupRegexKO_leftBracketMissing() {

		String regexIn = "123[a-b]*)";

		UDFSplitKeepList udf = new fr.aphp.wind.hive.udf.UDFSplitKeepList();
		assertFalse(udf.isValidRegexGroup(regexIn));
	}


}
