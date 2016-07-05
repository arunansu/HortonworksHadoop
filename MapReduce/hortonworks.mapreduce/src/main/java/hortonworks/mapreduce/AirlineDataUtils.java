package hortonworks.mapreduce;

import org.apache.hadoop.io.Text;

public class AirlineDataUtils {
	
	public static Boolean isHeader(Text line) {
		return line.toString().contains("AIRPORTCODE");
	}
	public static String getMonth(String[] contents) {
		return contents[1];
	}
	public static String getDayOfTheWeek(String[] contents) {
		return contents[2];
	}
	public static DelaysWritable parseDelaysWritable(String line) {
		DelaysWritable dw = new DelaysWritable();
		String[] contents = line.split(",");
		
		return dw;
	}
	public static Text parseDelaysWritableToText(DelaysWritable dw) {
		return new Text(dw.originAirportCode);
	}
	public static int getCustomPartition(MonthDoWWritable key, int indexRange, int noOfReducers) {
		return noOfReducers - 1;
	}
}
