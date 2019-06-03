package fr.aphp.wind.hive.udf;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class XTEAEncryptionEngineTest {

	@Before
	public void before() {

	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test_OK() throws InvalidKeyException, IOException {

		Random random = new Random(10000);
		
		String Key = "nxovsqnowwtdqbei";

		for (int i = 0; i <= 10; i++) {
			int expected = random.nextInt();
			long encrypted = XTEAEncryptionEngine.evaluate(expected, Key);
			int actual = XTEADecryptionEngine.evaluate(encrypted,Key);
			assertEquals(expected, actual);
		}

	}

	@Test
	public void test_random() throws InvalidKeyException, IOException {

		int input;
		String Key = "nxovsqnowwtdqbei";

		for (int i = 0; i < 1000000; i++) {
			input = (int) (Math.random() * 2147483647);
			long output = XTEAEncryptionEngine.evaluate(input, Key);
			int input2 = XTEADecryptionEngine.evaluate(output,Key);
			assertEquals(input, input2);
		}

	}

}
