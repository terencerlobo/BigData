package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.CalculateDPHRankingMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.FilterNTokenizeFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryDocumentFlatMapGroup;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryKeyMap;
import uk.ac.gla.dcs.bigdata.studentstructures.Document;

public class AssessedExercise {

	
	public static void main(String[] args) {
		
		// Set Hadoop directory path
		File hadoopDIR = new File("resources/hadoop/"); 
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); 
		
		// Get Spark master definition
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[4]"; 
		
		// Set Spark session name
		String sparkSessionName = "BigDataAE"; 
		
		// Create a SparkConf object and set the master and app name
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);

		// Create a SparkSession with the SparkConf object
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		// Get query file path	
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; 

		// Get news file path
		//TREC_Washington_Post_collection.v3.example
		//TREC_Washington_Post_collection.v2.jl.fix
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json"; 
		
		// Rank documents using Spark
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		spark.close();
		
		// Check if there are any rankings
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			// Create a directory to store the rankings
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write each DocumentRanking object to a file in the directory
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
	}
	
	// Method to rank documents using Spark
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {

		// Read query file and create a Dataset of Query objects
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Encoder<Query> queryEncoder = Encoders.bean(Query.class);
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), queryEncoder);
		
		// Read news file and create a Dataset of NewsArticle objects
		Dataset<Row> newsjson = spark.read().text(newsFile); 
		Encoder<NewsArticle> newsArticleEncoder = Encoders.bean(NewsArticle.class);
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), newsArticleEncoder); 
		
		// Convert the Query objects into a List
		List<Query> queryList = queries.collectAsList();
		
		// Create accumulators to store the total frequency of each term in the corpus and the total document length in the corpus
		CollectionAccumulator<HashMap<Query, Integer>> totalFrequencyInCorpus = spark.sparkContext().collectionAccumulator();
		LongAccumulator documentLengthInCorpus = spark.sparkContext().longAccumulator();
		
		// Filter and tokenize each NewsArticle and create a Dataset of Document objects
		// Create a FilterNTokenizeFlatMap object with the  queryList, totalFrequencyInCorpus, and documentLengthInCorpus.
		// Create an Encoder object for the Document class.
		// Apply the filterTokenizeMap to the news dataset using the flatMap transformation and the documentEncoder.
		FilterNTokenizeFlatMap filterTokenizeMap = new FilterNTokenizeFlatMap(queryList, totalFrequencyInCorpus, documentLengthInCorpus);
		Encoder<Document> documentEncoder = Encoders.bean(Document.class);
		Dataset<Document> tokenizedDocument = news.flatMap(filterTokenizeMap, documentEncoder);
		
		// Count the number of documents in the tokenizedDocument dataset.
		// Calculate the average document length in the corpus.
		// If totalFrequencyInCorpus is not null and its value is not null and its size is greater than 0,
		// set the totalFrequency to the first element of the totalFrequencyInCorpus value.
		long totalDocumentLength = tokenizedDocument.count();
		long averageDocumentLengthInCorpus = documentLengthInCorpus.value()/totalDocumentLength;
		HashMap<Query, Integer> totalFrequency = new HashMap<Query, Integer>();
		if(Objects.nonNull(totalFrequencyInCorpus) && Objects.nonNull(totalFrequencyInCorpus.value()) 
				&& totalFrequencyInCorpus.value().size() > 0) {		
			totalFrequency = totalFrequencyInCorpus.value().get(0);
		}
		
		// Create a CalculateDPHRankingMap object with the totalFrequency, averageDocumentLengthInCorpus, and totalDocumentLength.
		// Apply the dphRankingMap to the tokenizedDocument dataset using the flatMap transformation and the documentEncoder.
		CalculateDPHRankingMap dphRankingMap = new CalculateDPHRankingMap(totalFrequency, averageDocumentLengthInCorpus, totalDocumentLength);
		Dataset<Document> rankedDocuments = tokenizedDocument.flatMap(dphRankingMap, documentEncoder);
		
		QueryKeyMap queryKey = new QueryKeyMap();
		KeyValueGroupedDataset<Query, Document> queryKeyDataSet = rankedDocuments.groupByKey(queryKey, Encoders.bean(Query.class));
		queryKeyDataSet.count();
		
		// Create a QueryDocumentFlatMapGroup object.
		// Apply the documentoDR to the drMapKGD dataset using the flatMapGroups transformation and the DocumentRanking Encoder.
		QueryDocumentFlatMapGroup documenToDocumentRanking = new QueryDocumentFlatMapGroup();
		Dataset<DocumentRanking> documentRanking = queryKeyDataSet.flatMapGroups(documenToDocumentRanking, Encoders.bean(DocumentRanking.class));
		
		// Collect the result as a list of DocumentRanking objects.
		List<DocumentRanking> documentRankingList = documentRanking.collectAsList();

		// Print the documentRankingList to validate the results.
		System.out.println("*****documentRanking*****");
		System.out.println(documentRankingList);
		
		return documentRankingList; 
	}
	
	
}
