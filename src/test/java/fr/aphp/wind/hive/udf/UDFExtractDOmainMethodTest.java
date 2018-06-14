package fr.aphp.wind.hive.udf;

import junit.framework.Assert;

import java.util.LinkedList;

import org.junit.Test;

import brickhouse.udf.date.AddISOPeriodUDF;

public class UDFExtractDOmainMethodTest {

    @Test
    public void testConvertToCamelCase() {
        String t = "hellow{world}how";
        String pattern = "\\{.*?\\}";
    	 UDFsplitKeep udf = new fr.aphp.wind.hive.udf.UDFsplitKeep();
		 LinkedList<String> results = udf.evaluate(t, pattern);
		
		 LinkedList<String> result = new LinkedList<String>();
		 result.add("hellow");
		 result.add("{world}");
		 result.add("how");

        Assert.assertEquals(result, results);
    }

  

}
