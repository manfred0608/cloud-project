package edu.cmu.cs.cloudcomputing.zjers.etl.q2.censor;

import java.util.Arrays;
import java.util.Locale;

import org.apache.hadoop.util.StringUtils;

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
	
	private static final String VALID_CHARS="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
	
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
				
				// CSV quotation mark escape
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
	
	
	public static int getSentimentScore(String text) {
		String[] tweetToken = text.split("[^A-Za-z0-9]+");
		
		int score = 0;
		
		for (String token : tweetToken) {
			if (SentimentScoreMap.SCOREMAP.containsKey(token.toLowerCase(Locale.ENGLISH))) {
				score += SentimentScoreMap.SCOREMAP.get(token.toLowerCase(Locale.ENGLISH));
			}
		}
		
		return score;
	}
	
	public static String newLineToSemicolon(String str) {
		StringBuilder sb = new StringBuilder();
		
		for (int i = 0; i < str.length(); i++) {
			if (str.charAt(i) == '\n') {
				sb.append(";");
			} else {
				sb.append(str.charAt(i));
			}
		}
		
		sb.append(";");
		
		return sb.toString();
	}
}
