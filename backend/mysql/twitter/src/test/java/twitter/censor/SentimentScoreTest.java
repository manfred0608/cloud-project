package twitter.censor;

import junit.framework.TestCase;

public class SentimentScoreTest extends TestCase {

	public void testGetSentimentScore() {
		
		String[] testWords1 = {
				"can't",
				"stand"
		};
		assertEquals(-3, Censor.getSentimentScore(testWords1));
		
		String[] testWords2 = {
				"can't"
		};
		assertEquals(0, Censor.getSentimentScore(testWords2));
		
		String[] testWords3 = {
				"green",
				"fuck"
		};
		assertEquals(-4, Censor.getSentimentScore(testWords3));
		
		String[] testWords4 = {
				"green",
				"washing",
				"green"
		};
		assertEquals(-3, Censor.getSentimentScore(testWords4));
		
		
	}

}
