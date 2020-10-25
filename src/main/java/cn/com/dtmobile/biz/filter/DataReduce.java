package cn.com.dtmobile.biz.filter;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
@SuppressWarnings("rawtypes")
public class DataReduce extends Reducer<Text, Text, NullWritable, Text> {
	public MultipleOutputs mos;
	public final Text key = new Text();
	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs(context);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void reduce(Text inputKey, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		for (Text text : values) {
			String[] arr = text.toString().split(StringUtils.DELIMITER_BETWEEN_ITEMS2);
			String sdrType = arr[2];
			String imei = arr[4];
			String mcc = arr[6];
			String mnc = arr[7];
			String startTime = arr[8];
			String uetac = imei.length() >= 8 ? imei.substring(0,8) : "";
			String hour = startTime.replace("-", "").replace(" ", "").substring(0, 10);
			String city = "130600";

			String cgi = "";
			if(sdrType.equals("5")) {
				String eci = arr[23];
				long e = Long.parseLong(eci);
				String e1 = String.valueOf((int)(e / 256));
				String e2 = String.valueOf((int)(e % 256));
				cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
			}else if(sdrType.equals("6")){
				cgi = "";
			}else if(sdrType.equals("41")){
				String eci = arr[34];
				if (eci != null && !eci .equals("")){
					long e = Long.parseLong(eci);
					String e1 = String.valueOf((int)(e / 256));
					String e2 = String.valueOf((int)(e % 256));
					cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
				}
			}else if(sdrType.equals("42")){
				String eci = arr[26];
				if (eci != null && !eci .equals("")) {
					long e = Long.parseLong(eci);
					String e1 = String.valueOf((int)(e / 256));
					String e2 = String.valueOf((int)(e % 256));
					cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
				}
			}else if(sdrType.equals("43")){
				String eci = arr[33];
				if (StringUtils.isNumeric(eci)&& !eci .equals("")) {
					long e = Long.parseLong(eci);
					String e1 = String.valueOf((int)(e / 256));
					String e2 = String.valueOf((int)(e % 256));
					cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
				}
			}else if(sdrType.equals("21")){
				String eci = arr[24];
				if (eci != null && !eci .equals("")) {
					long e = Long.parseLong(eci);
					String e1 = String.valueOf((int)(e / 256));
					String e2 = String.valueOf((int)(e % 256));
					cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
				}
			}else if(sdrType.equals("22")){
				String eci = arr[23];
				if (eci != null && !eci .equals("")) {
					long e = Long.parseLong(eci);
					String e1 = String.valueOf((int)(e / 256));
					String e2 = String.valueOf((int)(e % 256));
					cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
				}
			}else if(sdrType.equals("24")){
				String eci = arr[28];
				if (eci != null && !eci .equals("")) {
					long e = Long.parseLong(eci);
					String e1 = String.valueOf((int)(e / 256));
					String e2 = String.valueOf((int)(e % 256));
					cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
				}
			}else if(sdrType.equals("25")){
				String eci = arr[23];
				long e = Long.parseLong(eci);
				String e1 = String.valueOf((int)(e / 256));
				String e2 = String.valueOf((int)(e % 256));
				cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
			}else if(sdrType.equals("26")){
				String eci = arr[24];
				long e = Long.parseLong(eci);
				String e1 = String.valueOf((int)(e / 256));
				String e2 = String.valueOf((int)(e % 256));
				cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
			}else if(sdrType.equals("27")){
				String eci = arr[23];
				long e = Long.parseLong(eci);
				String e1 = String.valueOf((int)(e / 256));
				String e2 = String.valueOf((int)(e % 256));
				cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
			}else if(sdrType.equals("28")){
				String eci = arr[24];
				long e = Long.parseLong(eci);
				String e1 = String.valueOf((int)(e / 256));
				String e2 = String.valueOf((int)(e % 256));
				cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
			}else if(sdrType.equals("29")){
				String eci = arr[23];
				long e = Long.parseLong(eci);
				String e1 = String.valueOf((int)(e / 256));
				String e2 = String.valueOf((int)(e % 256));
				cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
			}else if(sdrType.equals("33")){
				cgi = "";
			}else if(sdrType.equals("50")){
				cgi = "";
			}else if(sdrType.equals("51")){
				cgi = "";
			}else if(sdrType.equals("52")){
				cgi = "";
			}else if(sdrType.equals("53")){
				cgi = "";
			}else if(sdrType.equals("54")){
				cgi = "";
			}else if(sdrType.equals("55")){
				cgi = "";
			}else if(sdrType.equals("56")){
				cgi = "";
			}else if(sdrType.equals("57")){
				cgi = "";
			}else{
				cgi = "";
			}

			String arr1 = text.toString() + StringUtils.DELIMITER_VERTICAL + uetac +
					StringUtils.DELIMITER_VERTICAL + cgi +
					StringUtils.DELIMITER_VERTICAL + city +
					StringUtils.DELIMITER_VERTICAL + hour;

 			mos.write("s1mmeorgn", NullWritable.get(), new Text(arr1),"D:/classdata/output/result/tb_xdr_s1mme_"+ sdrType +"/p_city="+city+"/p_hour="+hour+"/s1_mme_"+city);

		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}




//	public MultipleOutputs mos;
//	public final Text key = new Text();
//	@SuppressWarnings("unchecked")
//	@Override
//	protected void setup(Context context) throws IOException,
//			InterruptedException {
//		mos = new MultipleOutputs(context);
//	}
//
//	@SuppressWarnings("unchecked")
//	@Override
//	public void reduce(Text inputKey, Iterable<Text> values, Context context)
//			throws IOException, InterruptedException {
//		for (Text text : values) {
//			String arr[] = text.toString().split(StringUtils.DELIMITER_BETWEEN_ELEMENT);
//			String tableName = arr[1];
//			String data = arr[0];
//			String city = data.split(StringUtils.DELIMITER_BETWEEN_ITEMS,-1)[1];
//			String[] days = context.getConfiguration().getStrings("hour") ;
//
//			String day = "day" ;
//			if(days != null){
//				day = days[0] ;
//			}
//
//			if (tableName.contains(TablesConstants.FILTER_S1MME)) {
//				mos.write("s1mmeorgn",  NullWritable.get(), new Text(data),"/jc_rc/rc_hive_db/d_ens_s1_mme_h_city/p_hour="+day+"/p_city="+city+"/s1_mme_"+city);
//			} else if (tableName.contains(TablesConstants.FILTER_HTTP)){
//				mos.write("s1uhttporgn",  NullWritable.get(), new Text(data),"/jc_rc/rc_hive_db/d_ens_http_4g_h_city/p_hour="+day+"/p_city="+city+"/s1u_http_"+city);
//			}
//		}
//
//	}
//
//	@Override
//	protected void cleanup(Context context) throws IOException,
//			InterruptedException {
//		mos.close();
//	}
}
