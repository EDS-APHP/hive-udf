package fr.aphp.wind.hive.udf;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UDFNormalizeAccentTest {

    private UDFNormalizeAccent udf;

    @Before
    public void before() {
        udf = new UDFNormalizeAccent();
    }

    @Test
    public void testNormalizeAccent() {
        String withAccent = "Les accents courants sont éè à ù Îîî ô";
        String withoutAccent = "Les accents courants sont ee a u Iii o";

        Assert.assertEquals(withoutAccent, udf.evaluate(withAccent));
    }

}