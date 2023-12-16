package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.Document;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryTFPair;

public class FilterNTokenizeFlatMap implements FlatMapFunction<NewsArticle,Document>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	// List of queries to be searched in the news articles
	private List<Query> queries;
	
	// Collection accumulator to store total frequency of query terms in corpus
	CollectionAccumulator<HashMap<Query, Integer>> totalFrequencyInCorpus;
	
	// Long accumulator to store the length of documents in the corpus
	LongAccumulator documentLengthInCorpus;
	
	// Constructor to initialize the variables
	public FilterNTokenizeFlatMap (List<Query> queries, CollectionAccumulator<HashMap<Query, Integer>> totalFrequencyInCorpus, 
			LongAccumulator documentLengthInCorpus) {
		this.queries = queries;
		this.totalFrequencyInCorpus = totalFrequencyInCorpus;
		this.documentLengthInCorpus = documentLengthInCorpus;
	}
	
	
	@Override
	public Iterator<Document> call(NewsArticle newsArticle) throws Exception {
		List<ContentItem> finalContents = new ArrayList<ContentItem>();
		
		TextPreProcessor preProcessor = new TextPreProcessor();
		
		List<Document> documentList = new ArrayList<Document>();
		
		Document document = new Document();
		List<String> contentList = new ArrayList<String>();
		
		if(Objects.nonNull(newsArticle.getTitle())) {
			
			// We are preprocessing the data here. We are filtering out the null data, and adding only the first 5 to the content list. 
			Optional.ofNullable(newsArticle.getContents()).orElse(Collections.emptyList()).stream().forEach(content ->{
				if(Objects.nonNull(content) && !StringUtils.isEmpty(content.getSubtype()) && "paragraph".equals(content.getSubtype().toLowerCase()) 
						&& Objects.nonNull(content)){
					if(finalContents.size() < 5) {
						finalContents.add(content);
						newsArticle.setContents(finalContents);
						contentList.addAll(preProcessor.process(content.getContent().toLowerCase()));
					}
				}
			});
			contentList.addAll(preProcessor.process(newsArticle.getTitle().toLowerCase()));
			
			document.setTokenizedContents(contentList);
			document.setNewsArticle(newsArticle);
			document.setCurrentDocumentLength(document.getTokenizedContents().size());
			
			List<String> tokens = document.getTokenizedContents();
			
			
			// This code takes a list of queries and tokens, and updates the incoming document with their associated query term frequency pairs.
			// If total frequency in corpus is provided, it updates the frequency count for each query.
			// If a query term is found in a token, it increments the term frequency count for the query in the document's query term frequency pair.
			// If a document matches with more than one query, it creates a new document object with the same data, but different query parameter and adds to the dataset. 
			// So, if query 1 matches with the document, and if query 2 matches with the document, both the matches are stored in the document object.
			Optional.ofNullable(queries).orElse(Collections.emptyList()).stream().forEach(query -> {
				if(Objects.nonNull(query.getQueryTerms()) && query.getQueryTerms().size() > 0) {
					List<String> queryTerms = query.getQueryTerms();
					queryTerms.stream().forEach(queryTerm -> {
						tokens.stream().forEach(token -> {
							if(token.toLowerCase().toString().equals(queryTerm.toLowerCase().toString())) {
								if(Objects.isNull(document.getQueryTFPair())) {
									QueryTFPair queryPair = new QueryTFPair();
									queryPair.setQuery(query);
									queryPair.setTermFrequency(1);
									document.setQueryTFPair(queryPair);
									
									if(Objects.nonNull(totalFrequencyInCorpus) && Objects.nonNull(totalFrequencyInCorpus.value()) 
											&& totalFrequencyInCorpus.value().size() > 0)  {
										if(Objects.nonNull(totalFrequencyInCorpus.value().get(0).get(queryPair.getQuery()))){
											int currentValue = (int)totalFrequencyInCorpus.value().get(0).get(queryPair.getQuery());
											totalFrequencyInCorpus.value().get(0).replace(queryPair.getQuery(), currentValue+queryPair.getTermFrequency());
										}
										else {
											if(Objects.nonNull(totalFrequencyInCorpus) && Objects.nonNull(totalFrequencyInCorpus.value()) 
													&& totalFrequencyInCorpus.value().size() > 0) {
												totalFrequencyInCorpus.value().get(0).put(queryPair.getQuery(), queryPair.getTermFrequency());
											}
										}
									}
									else {
										HashMap<Query, Integer> newQuery = new HashMap<Query, Integer>();
										newQuery.put(queryPair.getQuery(), queryPair.getTermFrequency());
										totalFrequencyInCorpus.add(newQuery);
									}

								}
								else {
									if(document.getQueryTFPair().getQuery().getOriginalQuery().toLowerCase().equals(query.getOriginalQuery().toLowerCase())) {
										document.getQueryTFPair().setTermFrequency(document.getQueryTFPair().getTermFrequency() + 1);
										if(Objects.nonNull(totalFrequencyInCorpus) 
												&& Objects.nonNull(totalFrequencyInCorpus.value()) 
												&& totalFrequencyInCorpus.value().size() > 0) {
											int currentValue = (int)totalFrequencyInCorpus.value().get(0).get(document.getQueryTFPair().getQuery());
											totalFrequencyInCorpus.value().get(0).replace(document.getQueryTFPair().getQuery(), 
													currentValue+document.getQueryTFPair().getTermFrequency());
											
										}
									}
									else {
										Document newDocument = new Document();
										newDocument.setCurrentDocumentLength(document.getCurrentDocumentLength());
										newDocument.setNewsArticle(document.getNewsArticle());
										QueryTFPair queryPair = new QueryTFPair();
										queryPair.setQuery(query);
										queryPair.setTermFrequency(1);
										newDocument.setQueryTFPair(queryPair);
										newDocument.setTokenizedContents(document.getTokenizedContents());
										documentList.add(newDocument);
										if(Objects.nonNull(totalFrequencyInCorpus.value().get(0).get(queryPair.getQuery()))){
											int currentValue = (int)totalFrequencyInCorpus.value().get(0).get(queryPair.getQuery());
											totalFrequencyInCorpus.value().get(0).replace(queryPair.getQuery(), currentValue+queryPair.getTermFrequency());
										}
										else {
											if(Objects.nonNull(totalFrequencyInCorpus) && Objects.nonNull(totalFrequencyInCorpus.value()) 
													&& totalFrequencyInCorpus.value().size() > 0) {
												totalFrequencyInCorpus.value().get(0).put(queryPair.getQuery(), queryPair.getTermFrequency());
											}
										}
									}
									
									
								}
							}
							
						});
					});
				}
			});
			
			document.setCurrentDocumentLength(document.getTokenizedContents().size());
			documentLengthInCorpus.add(document.getCurrentDocumentLength());
			documentList.add(document);
			
		}
		
		return documentList.iterator();
	}

}
