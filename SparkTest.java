import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkTest {
	public static void main(String[] args)throws Exception{
		if(args.length != 2) {
			System.err.println("Usage:SparkTest <input path> <output path>");
			System.exit(-1);
		}
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext("yarn","SparkTest",conf);
		//input file as lines
		JavaRDD<String> lines = sc.textFile(args[0]);
		//map the lines as <rec1,rec2>
		JavaRDD<String[]> records = lines.map(
				s -> s.split(" "));
		//filter records
		JavaRDD<String[]> filter = records.filter(
				rec ->!rec[1].equalsIgnoreCase("999"));
		//set the records to Tuple2<key,value>
		JavaPairRDD<Integer, Integer> tuples = filter.mapToPair(
				rec -> new Tuple2<Integer,Integer>
				(Integer.parseInt(rec[0]),Integer.parseInt(rec[1])));
		//do the reduce
		JavaPairRDD<Integer, Integer> maxTemps = tuples.reduceByKey(
				(Integer arg0,Integer arg1) ->
				(Math.max(arg0,arg1)));
		//not use lambada
//		JavaRDD<String[]> records = lines.map(new Function<String,String[]>() {
//			private static final long serialVersionUID = 1L;
//			@Override public String[] call(String s) {
//				return s.split(" ");
//			}
//		});
//		JavaRDD<String[]> filter = records.filter(new Function<String[],Boolean>() {
//			private static final long serialVersionUID = 1L;
//			@Override public Boolean call(String[] rec) {
//				return !rec[1].equalsIgnoreCase("999"); 
//			}
//		});
		//set the records to Tuple2<key,value>
//		JavaPairRDD<Integer,Integer> tuples = filter.mapToPair(
//				new PairFunction<String[], Integer, Integer>() {
//					private static final long serialVersionUID = 1L;
//			@Override public Tuple2<Integer, Integer> call(String[] rec) {
//				return new Tuple2<Integer, Integer>(
//						Integer.parseInt(rec[0]),Integer.parseInt(rec[1]));
//			}
//		});
//		JavaPairRDD<Integer, Integer> maxTemps = tuples.reduceByKey(
//				new Function2<Integer, Integer, Integer>() {
//					private static final long serialVersionUID = 1L;
//			@Override
//			public Integer call(Integer arg0, Integer arg1) throws Exception {
//				// TODO Auto-generated method stub
//				return Math.max(arg0, arg1);
//			}
//		});
		maxTemps.saveAsTextFile(args[1]);
		sc.stop();
		sc.close();
	}
}
