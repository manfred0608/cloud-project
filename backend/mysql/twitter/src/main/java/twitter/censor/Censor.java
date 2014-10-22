package twitter.censor;

import java.util.Arrays;
import java.util.Locale;

import org.apache.hadoop.util.StringUtils;

import twitter.censor.SentimentScoreMap.PhraseScore;

public class Censor {

	public static String censor(String[] originalWords) {
		for (int i = 0; i < originalWords.length; i++) {
			String thisWord = originalWords[i];
			if (thisWord.length() <= 2) continue;
			if (WordList.WORDSET.contains(thisWord.toLowerCase(Locale.ENGLISH))) {
				System.out.println("Found Word: " + thisWord);
				String[] asterisks = new String[thisWord.length() - 2];
				Arrays.fill(asterisks, "*");
				originalWords[i] = thisWord.charAt(0) + StringUtils.join("", asterisks) + thisWord.charAt(thisWord.length() - 1);
				System.out.println("Censored: " + originalWords[i]);
			}
		}
		
		return StringUtils.join(" ", originalWords);
	}
	
	public static int getSentimentScore(String[] words) {
		int score = 0;
		for (int i = 0; i < words.length; i++) {
			String word = words[i];
			if (SentimentScoreMap.PHRASEMAP.containsKey(word)) {
				PhraseScore ps = SentimentScoreMap.PHRASEMAP.get(word);
				StringBuilder phrase = new StringBuilder();
				
				if (i + ps.LENGTH <= words.length) {
				
					for (int j = i; j < i + ps.LENGTH; j++) {
						phrase.append(words[j]);
					}
					
					System.out.println("phrase:" + phrase.toString());
					for (int j = 0; j < ps.CONCATENATEDPHRASE.length; j++) {
						System.out.println("Comparing " + phrase + " with " + ps.CONCATENATEDPHRASE[i]);
						if (phrase.toString().equalsIgnoreCase(ps.CONCATENATEDPHRASE[j])) {
							score += ps.SCORE;
							i++;
							word = words[i];
							break;
						}
					}
				}
			}
			
			if (SentimentScoreMap.SCOREMAP.containsKey(word)) {
				score += SentimentScoreMap.SCOREMAP.get(word);
			}
		}
		
		return score;
	}
}
