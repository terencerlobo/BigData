package uk.ac.gla.dcs.bigdata.studentfunctions;

import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;

public class MainMethod {

	
	public static void main(String[] args) {
		double score = DPHScorer.getDPHScore((short) 2, 4, 161, 4798, 85);
		System.out.println(score + " ==> Score..");
	}
	
}
