import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Tests implements Comparable<Tests>{
	private int i = 1;
	@SuppressWarnings("unused")
	private enum T{
		a("A"),b("B"),c("C"),d("D");
		@SuppressWarnings("unused")
		private String string;
		private T(String string) {this.string = string;}
	}
	public Tests(int i) {
		this.i = i;
	}
	public void setI(int i) {
		this.i = i;
	}
	public static void main(String[] args) throws Exception{
		SparkSession spark = SparkSession
				.builder()
				.appName("test")
				.master("local[2]")
				.getOrCreate();
		Dataset<Row> df = spark.read().json("/home/hadoop/Desktop/spark.txt");
		df.createOrReplaceTempView("test");
		df.sqlContext().sql("select url,ts from test where type like 'c%' order by ts").show();
		ArrayList<Tests> arrayList = new ArrayList<>();
		arrayList.add(new Tests(10));
		arrayList.add(new Tests(9));
		Iterator<Tests> it = arrayList.iterator();
		while (it.hasNext()) {
			System.out.println(it.next().i);
		}
		Collections.sort(arrayList);
		Iterator<Tests> its = arrayList.iterator();
		while (its.hasNext()) {
			System.out.println(its.next().i);
		}
	}
	public int compareTo(Tests tests) {
		Tests t = tests;
		return Integer.compare(i, t.i);
	}
}

