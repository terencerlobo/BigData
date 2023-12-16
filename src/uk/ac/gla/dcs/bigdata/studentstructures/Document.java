package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class Document implements Serializable, Comparable<Document>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private NewsArticle newsArticle;
	private List<String> tokenizedContents;
	private QueryTFPair queryTFPair;
	private int currentDocumentLength;
	private double score;
	private double totalTFInCorpus;
	
	public NewsArticle getNewsArticle() {
		return newsArticle;
	}
	public void setNewsArticle(NewsArticle newsArticle) {
		this.newsArticle = newsArticle;
	}
	
	public List<String> getTokenizedContents() {
		return tokenizedContents;
	}
	public void setTokenizedContents(List<String> tokenizedContents) {
		this.tokenizedContents = tokenizedContents;
	}
	
	public QueryTFPair getQueryTFPair() {
		return queryTFPair;
	}
	public void setQueryTFPair(QueryTFPair queryTFPair) {
		this.queryTFPair = queryTFPair;
	}
	
	public int getCurrentDocumentLength() {
		return currentDocumentLength;
	}
	public void setCurrentDocumentLength(int currentDocumentLength) {
		this.currentDocumentLength = currentDocumentLength;
	}
	
	public double getScore() {
		return score;
	}
	public void setScore(double score) {
		this.score = score;
	}
	
	public double getTotalTFInCorpus() {
		return totalTFInCorpus;
	}
	public void setTotalTFInCorpus(double totalTFInCorpus) {
		this.totalTFInCorpus = totalTFInCorpus;
	}
	
	@Override
	public int compareTo(Document o) {
		return new Double(score).compareTo(o.score);
	}
	
	
}
