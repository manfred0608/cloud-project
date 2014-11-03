package edu.cmu.cs.cloudcomputing.zjers.etl.q2.censor;

import edu.cmu.cs.cloudcomputing.zjers.etl.q2.censor.Censor;
import junit.framework.TestCase;

public class SentimentScoreTest extends TestCase {

	public void testGetSentimentScore() {
		
		String tweetText = "What the fuck";
		
		assertEquals(-4,  Censor.getSentimentScore(tweetText));
		
		String[] testWords1 = {
				"Can't",
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
	
	public void testCensor() {
		String testString1 = "What  the fuck";
		String censored = Censor.censor(testString1);
		System.out.println(censored);
		assertTrue(censored.equals("What  the f**k"));
	}

}
