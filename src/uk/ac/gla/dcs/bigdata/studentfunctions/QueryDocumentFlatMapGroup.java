package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;

import com.google.common.collect.Lists;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.Document;

public class QueryDocumentFlatMapGroup implements FlatMapGroupsFunction<Query,Document,DocumentRanking>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Iterator<DocumentRanking> call(Query key, Iterator<Document> documents) throws Exception {
			List<DocumentRanking> documentRankingList = new ArrayList<DocumentRanking>();
			DocumentRanking documentRanking = new DocumentRanking();
			
			List<Document> docList = Lists.newArrayList(documents);
			Collections.sort(docList);
			Collections.reverse(docList);
			List<RankedResult> rankedResults = new ArrayList<RankedResult>();
			
			int loopTill = 0;
			
			loopTill = docList.size() < 10? docList.size(): 10; 
			List<Integer> similarityIndx = new ArrayList<Integer>();
			// This is the curx of extracting the top 10. The logic is to first sort and filter the top 10 and see if the title matches. 
			// If any of the title matches, then we just ignore the low ranked title, and fetch the next item from the list. 
			// The loopTill variable determines the number of times we need to loop.
			// If at all the query has more than one result, the if block will be executed. But if the query contains only one, the else block will be executed.
			// This detailed logic takes care of all the edge case scenarios.
			if(loopTill > 1) {
				for(int indx = 0; indx < loopTill; indx++) {
					Document document = docList.get(indx);
					boolean isDocSimilar = false;
					if(document.getQueryTFPair().getQuery().getOriginalQuery().equals(key.getOriginalQuery())) {
						int j = indx+1 < loopTill? indx+1 : indx;
						for(int jIndx = j; jIndx < loopTill; jIndx++) {
							String title1 = docList.get(indx).getNewsArticle().getTitle();
							String title2 = docList.get(jIndx).getNewsArticle().getTitle();
							
							double similarityScore = TextDistanceCalculator.similarity(title1, title2);
							if(similarityScore < 0.5) {
								isDocSimilar = true;
								similarityIndx.add(jIndx);
								break;
							}
						}
						if(!similarityIndx.contains(indx)) {
							RankedResult rankedResult = new RankedResult();
							rankedResult.setArticle(docList.get(indx).getNewsArticle());
							rankedResult.setDocid(docList.get(indx).getNewsArticle().getId());
							rankedResult.setScore(docList.get(indx).getScore());
							if(rankedResults.size() < 10) {
								rankedResults.add(rankedResult);
							}
						}
						if(isDocSimilar) {
							indx = indx + 1;
						}
					}
					
					if(rankedResults.size() < 10 && loopTill < docList.size()) {
						loopTill = loopTill + 1;
					}
				}
			}
			else {
				RankedResult rankedResult = new RankedResult();
				rankedResult.setArticle(docList.get(0).getNewsArticle());
				rankedResult.setDocid(docList.get(0).getNewsArticle().getId());
				rankedResult.setScore(docList.get(0).getScore());
				rankedResults.add(rankedResult);
			}
			
			documentRanking.setQuery(key);
			documentRanking.setResults(rankedResults);
			documentRankingList.add(documentRanking);
			
					
			return documentRankingList.iterator();
		
	}
	
	

}
