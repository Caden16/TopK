import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 
 * @author Liyoung
 * 
 */
public class Topk {

	private static final Pattern SPACE = Pattern.compile("\\s+");

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.println("Usage: TopKCountWord <infile> <stopword> <outfile>");
			System.exit(1);
		}

		String inFile = args[0]; // Path of DataSet.
		String stopWordpath = args[1]; // Path of stopWords.
		String outFile = args[2]; // Path of the file of the final result.

		final Set<String> stopWord = new HashSet<>(); // Store all stopWord in a
														// HashSet.
		try (FileReader fr = new FileReader(stopWordpath);
				BufferedReader reader = new BufferedReader(fr);) {
			String line = null;

			// Each line of the file stopWords.txt contains a blank space,so we
			// use a loop to split this blank space;
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				stopWord.add(line);
			}
		} catch (IOException e) {
		}

		JavaSparkContext context = new JavaSparkContext("local", "Topk");

		// Read the DataSet from the HDFS into a JavaRDD;
		JavaRDD<String> lines = context.textFile(inFile);

		/*
		 * Filter all punctuation.
		 */
		JavaRDD<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<String> call(String s) {
						s = s.replaceAll("\\pP", " ");
						return Arrays.asList(SPACE.split(s));
					}
				});

		/*
		 * Filter the words contained in stopWords.
		 */
		JavaRDD<String> afterFilter = words
				.filter((new Function<String, Boolean>() {					
					private static final long serialVersionUID = 1L;

					public Boolean call(String w) {
						return !stopWord.contains(w);
					}
				}));

		/*
		 * The process of map, transform the RDD "afterFilter" into a PairRDD,
		 * which consist of <key,value> sets.
		 */
		JavaPairRDD<String, Integer> ones = afterFilter
				.mapToPair(new PairFunction<String, String, Integer>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});
		/*
		 * The process of reduce, reduce the value in the key-value pair
		 * produced by the previous step.
		 */
		JavaPairRDD<String, Integer> counts = ones
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});

		// Take order and sorts the top 100.
		List<Tuple2<String, Integer>> output = (List<Tuple2<String, Integer>>) counts
				.takeOrdered(101, new TupleComparator());

		/*
		 * Save the result to local file system.
		 */
		try {
			FileWriter fw = new FileWriter(outFile);
			@SuppressWarnings("resource")
			BufferedWriter bw = new BufferedWriter(fw);
			for (int i = 1; i < output.size(); i++) {
				Tuple2<?, ?> tuple = output.get(i);
				bw.write(tuple._1() + "\n");
			}
			bw.flush();
		} catch (IOException e) {

		}
		context.stop();
	}
/**
 * 
 * @author Liyoung
 * The function of this class is to sort a set of tuple.
 */
	public static class TupleComparator implements
			Comparator<Tuple2<String, Integer>>, Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		public int compare(Tuple2<String, Integer> o1,
				Tuple2<String, Integer> o2) {
			return Integer.compare(o2._2(), o1._2());
		}
	}

}