package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.Document;

public class CalculateDPHRankingMap implements FlatMapFunction<Document, Document>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	// Initialize variables
	HashMap<Query, Integer> totalFrequency; // A map that stores the frequency of each query in the entire corpus
	double averageDocumentLengthInCorpus; // Average document length in the corpus
	long totalDocsInCorpus; // Total number of documents in the corpus
	
	// Constructor
	public CalculateDPHRankingMap (HashMap<Query, Integer> totalFrequency, 
			double averageDocumentLengthInCorpus, long totalDocsInCorpus) {
		this.totalFrequency = totalFrequency;
		this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
		this.totalDocsInCorpus = totalDocsInCorpus;
	}
	
	
	@Override
	public Iterator<Document> call(Document document) throws Exception {
		List<Document> documents = new ArrayList<Document>();
		short termFrequencyInCurrentDocument  = 0;

		// Here we fetch the corpus frequency from the CollectionAccumulator, and then use that for the DPH Calculation. 
		if(Objects.nonNull(document.getQueryTFPair())) {
			int corpusFrequency = 0;
			for (Map.Entry<Query, Integer> entry : totalFrequency.entrySet()) {
			    if(entry.getKey().getOriginalQuery().equals(document.getQueryTFPair().getQuery().getOriginalQuery())) {
			    	corpusFrequency = entry.getValue();
			    }
			}
			
			// Here we are invoking the DPH scorer with all the required parameters and setting it in the document object, which we will process in the next step of the flow.
			termFrequencyInCurrentDocument = (short)document.getQueryTFPair().getTermFrequency();
			if(termFrequencyInCurrentDocument != 0 && corpusFrequency != 0 && averageDocumentLengthInCorpus != 0 && totalDocsInCorpus != 0 && document.getCurrentDocumentLength() != 0) {
				double dphScore = DPHScorer.getDPHScore(termFrequencyInCurrentDocument, corpusFrequency, document.getCurrentDocumentLength(), 
						averageDocumentLengthInCorpus, totalDocsInCorpus);
				document.setScore(dphScore);
				documents.add(document);
			}
		}
		
		
		return documents.iterator();
	}

}
