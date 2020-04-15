package fr.aphp.wind.hive.udf;


import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

public class UDFLevenshteinDistanceTest {

    private UDFLevenshteinDistance udf;

    @Before
    public void before() {
        udf = new UDFLevenshteinDistance();
    }

    @Test
    public void computeDistance() {
        String c1 = "azerty";
        Assert.assertEquals((double) 0, udf.evaluate(c1, c1));
        Assert.assertEquals((double) c1.length(), udf.evaluate(c1, c1.concat(c1)));
    }
}
