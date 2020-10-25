package cn.com.dtmobile.biz.filter;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DataMap extends Mapper<LongWritable, Text, Text, Text> {

	private final Text key = new Text();

	@Override
	protected void map(LongWritable keys, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		if (value.getLength() > 0) {
//			String filePath = context.getInputSplit().toString().toUpperCase();
//			System.out.println(filePath);
			String[] values = value.toString().split(StringUtils.DELIMITER_BETWEEN_ITEMS2);
			if(values.length > 3){
				key.set(values[3]);
				context.write(key,new Text(value));
			}
		}
	}

//	private final Text key = new Text();
//	@Override
//	public void map(LongWritable keys, Text value, Context context)
//			throws IOException, InterruptedException {
//
//		if (value.getLength() > 0) {
//			String filePath = context.getInputSplit().toString().toUpperCase();
//			String[] values = value.toString().split(StringUtils.DELIMITER_VERTICAL,-1);
//
//
//			if (filePath.contains("S1MME") && values.length > 0) {
//				if (StringUtils.isNotEmpty(values[5]) && !"\\N".equals(values[5]) && !"\\N".equals(values[1])){
//					key.set(values[5]);
//					context.write(key, new Text(value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_S1MME));
//				}
//			}else if (filePath.contains("HTTP") && values.length > 20) {
//				String timeStamp = values[19];
//				String date = Util.getDateTime(timeStamp);
//				String days = context.getConfiguration().get("hour");
//
//				if ((date != null) && (days != null) && (date.equals(days))) {
//					if (StringUtils.isNotEmpty(values[5]) && !"\\N".equals(values[5]) && !"\\N".equals(values[1])){
//						key.set(values[5]);
//
//						/**
//						 * 5  imsi
//						 * 1  city
//						 * 6  imei
//						 * 7  msisdn
//						 * 16 cell_id
//						 * 19 starttime
//						 * 20 endtime
//						 * 26 user_ipv4
//						 * 58 host
//						 * 59 uri
//						 * 76 http_content
//						 *
//						 */
//
//
//						StringBuffer sb = new StringBuffer() ;
//
//						sb.append(values[5])  ;
//						sb.append(StringUtils.DELIMITER_BETWEEN_ITEMS) ;
//						sb.append(values[1])  ;
//						sb.append(StringUtils.DELIMITER_BETWEEN_ITEMS) ;
//						sb.append(values[6])  ;
//						sb.append(StringUtils.DELIMITER_BETWEEN_ITEMS) ;
//						sb.append(values[7])  ;
//						sb.append(StringUtils.DELIMITER_BETWEEN_ITEMS) ;
//						sb.append(values[16]) ;
//						sb.append(StringUtils.DELIMITER_BETWEEN_ITEMS) ;
//						sb.append(values[19]) ;
//						sb.append(StringUtils.DELIMITER_BETWEEN_ITEMS) ;
//						sb.append(values[20]) ;
//						sb.append(StringUtils.DELIMITER_BETWEEN_ITEMS) ;
//						sb.append(values[26]) ;
//						sb.append(StringUtils.DELIMITER_BETWEEN_ITEMS) ;
//						sb.append(values[58]) ;
//						sb.append(StringUtils.DELIMITER_BETWEEN_ITEMS) ;
//						sb.append(values[59]) ;
//						sb.append(StringUtils.DELIMITER_BETWEEN_ITEMS) ;
//						sb.append(values[76]) ;
//
//
//						context.write(key, new Text( sb + StringUtils.DELIMITER_BETWEEN_ELEMENT + TablesConstants.FILTER_HTTP));
//					}
//				}
//			}
//		}

//	}



}
