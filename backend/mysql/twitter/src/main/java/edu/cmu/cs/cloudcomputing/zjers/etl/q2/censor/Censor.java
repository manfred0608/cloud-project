package edu.cmu.cs.cloudcomputing.zjers.etl.q2.censor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.util.StringUtils;

import edu.cmu.cs.cloudcomputing.zjers.etl.q2.censor.SentimentScoreMap.PhraseScore;

public class Censor {
	
	public static String escapeCSV(String str) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < str.length(); i++) {
			if (str.charAt(i) == '"') {
				sb.append('\\');
			}
			sb.append(str.charAt(i));
		}
		
		return sb.toString();
	}

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
	
	public static String censorWord(String word) {
		if (word.length() <= 2) return word;
		
		String result = word;
		if (WordList.WORDSET.contains(word.toLowerCase(Locale.ENGLISH))) {
//			System.out.println("Found Word: " + word);
			String[] asterisks = new String[word.length() - 2];
			Arrays.fill(asterisks, "*");
			result = word.charAt(0) + StringUtils.join("", asterisks) + word.charAt(word.length() - 1);
//			System.out.println("Censored: " + result);
		}
		
		return result;
	}
	
	public static String censor(String text) {

		int start = 0, end = 0;
		StringBuilder sb = new StringBuilder();
		
		for (int i = 0; i < text.length(); i++) {
			if (VALID_CHARS.indexOf(text.charAt(i)) != -1) {
				end++;
			} else {
				if (start != end) {
					String before = text.substring(start, end);
					String after = censorWord(before);
//					System.out.println("Censoring " + before);
//					System.out.println("Got " + after);
					sb.append(after);
				}
				end = i + 1;
				start = i + 1;
				
				if (text.charAt(i) == '"') sb.append('\\');
				sb.append(text.charAt(i));
			}
		}

		if (start != end) {
			String before = text.substring(start, end);
			String after = censorWord(before);
//			System.out.println("Censoring " + before);
//			System.out.println("Got " + after);
			sb.append(after);
		}
		
		return sb.toString();
	}
	
	private static final String VALID_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'";
	public static int getSentimentScore(String text) {
		List<String> list = new ArrayList<String>();
		int start = 0, end = 0;
		for (int i = 0; i < text.length(); i++) {
			if (VALID_CHARS.indexOf(text.charAt(i)) != -1) {
				end++;
			} else {
				if (start != end) list.add(text.substring(start, end));
				end = i + 1;
				start = i + 1;
			}
		}
		if (start != end) list.add(text.substring(start, end));
		
		return getSentimentScore(list.toArray(new String[list.size()]));
	}
	
	public static int getSentimentScore(String[] words) {
		int score = 0;
		for (int i = 0; i < words.length; i++) {
			String word = words[i].toLowerCase(Locale.ENGLISH);
//			System.out.println("words[" + i + "]: " + word);
			if (word.isEmpty()) continue;
//			System.out.println("Word: " + word);
			if (SentimentScoreMap.PHRASEMAP.containsKey(word)) {
				PhraseScore ps = SentimentScoreMap.PHRASEMAP.get(word);
				StringBuilder phrase = new StringBuilder();
				
				if (i + ps.LENGTH <= words.length) {
				
					for (int j = i; j < i + ps.LENGTH; j++) {
						phrase.append(words[j]);
					}
					
//					System.out.println("phrase:" + phrase.toString());
					for (int j = 0; j < ps.CONCATENATEDPHRASE.length; j++) {
//						System.out.println("Comparing " + phrase + " with " + ps.CONCATENATEDPHRASE[i]);
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
