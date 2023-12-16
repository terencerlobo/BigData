package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.Document;

public class QueryKeyMap implements MapFunction<Document,Query>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Query call(Document value) throws Exception {
		return value.getQueryTFPair().getQuery();
	}

}
