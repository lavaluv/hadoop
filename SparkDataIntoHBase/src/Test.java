
public class Test {
	@SuppressWarnings("unused")
	public static void main(String args[])throws Exception{
		byte[] row = new byte[0];
		int bucketCount = 4;
		RowKeyDistributorByBucket kb = new RowKeyDistributorByBucket(bucketCount);
		System.out.println(RowKeyConverter.makeInfoRowKey("1325844454", "201806150000"));
	}
}
