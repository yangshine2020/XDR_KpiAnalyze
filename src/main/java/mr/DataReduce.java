package mr;

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
	public void reduce(Text inputKey, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text text : values) {
			String arr[] = text.toString().split(StringUtils.DELIMITER_BETWEEN_ITEMS);
			String tableName = arr[1];
			String data = arr[0];
			String[] datas = data.split(StringUtils.DELIMITER_INNER_ITEM);

			if(arr.length > 1){
				if (tableName.contains(TablesConstants.FILTER_S1MME)) {
//					mos.write("s1mmeorgn",  NullWritable.get(), new Text(data),"D:/classdata/output2/d_ens_s1_mme_h_city/p_hour=");
				} else if (tableName.contains(TablesConstants.FILTER_HTTP)){
					String start_t = datas[1];
					String imei = datas[5];
					String eci = datas[8];
					String mcc = datas[35];
					String mnc = datas[36];
					String uetac = imei.length() >= 8 ? imei.substring(0,8) : "";
					String hour = start_t.replace("-", "").replace(" ", "").substring(0, 10);
					String city = "130600";
					String cgi = "";
					if (eci != null && !eci .equals("")) {
						long e = Long.parseLong(eci);
						String e1 = String.valueOf((int)(e / 256));
						String e2 = String.valueOf((int)(e % 256));
						cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
					}
					String arr1 = data +
							StringUtils.DELIMITER_VERTICAL + uetac +
							StringUtils.DELIMITER_VERTICAL + cgi +
							StringUtils.DELIMITER_VERTICAL + city +
							StringUtils.DELIMITER_VERTICAL + hour;

					mos.write("s1uhttporgn",  NullWritable.get(), new Text(arr1),"D:/classdata/output2/tb_xdr_s1u_http/p_city="+city+"/p_hour="+hour+"/s1u_http_"+city);
				}else if (tableName.contains(TablesConstants.FILTER_OTHER)){
					String start_t = datas[1];
					String imei = datas[5];
					String eci = datas[8];
					String mcc = datas[35];
					String mnc = datas[36];
					String uetac = imei.length() >= 8 ? imei.substring(0,8) : "";
					String hour = start_t.replace("-", "").replace(" ", "").substring(0, 10);
					String city = "130600";
					String cgi = "";
					if (eci != null && !eci .equals("")) {
						long e = Long.parseLong(eci);
						String e1 = String.valueOf((int)(e / 256));
						String e2 = String.valueOf((int)(e % 256));
						cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
					}
					String arr1 = data +
							StringUtils.DELIMITER_VERTICAL + uetac +
							StringUtils.DELIMITER_VERTICAL + cgi +
							StringUtils.DELIMITER_VERTICAL + city +
							StringUtils.DELIMITER_VERTICAL + hour;

					mos.write("s1uotherorgn",  NullWritable.get(), new Text(arr1),"D:/classdata/output2/tb_xdr_s1u_other/p_city="+city+"/p_hour="+hour+"/s1u_other_"+city);
				}else if (tableName.contains(TablesConstants.FILTER_IM)){
					String start_t = datas[1];
					String imei = datas[5];
					String eci = datas[9];
					String mcc = datas[24];
					String mnc = datas[25];
					String uetac = imei.length() >= 8 ? imei.substring(0,8) : "";
					String hour = start_t.replace("-", "").replace(" ", "").substring(0, 10);
					String city = "130600";
					String cgi = "";
					if (eci != null && !eci .equals("")) {
						long e = Long.parseLong(eci);
						String e1 = String.valueOf((int)(e / 256));
						String e2 = String.valueOf((int)(e % 256));
						cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
					}
					String arr1 = data +
							StringUtils.DELIMITER_VERTICAL + uetac +
							StringUtils.DELIMITER_VERTICAL + cgi +
							StringUtils.DELIMITER_VERTICAL + city +
							StringUtils.DELIMITER_VERTICAL + hour;

					mos.write("s1uimorgn",  NullWritable.get(), new Text(arr1),"D:/classdata/output2/tb_xdr_s1u_im/p_city="+city+"/p_hour="+hour+"/s1u_other_"+city);
				}else if (tableName.contains(TablesConstants.FILTER_GAME)){
					String start_t = datas[1];
					String imei = datas[5];
					String eci = datas[9];
					String mcc = datas[24];
					String mnc = datas[25];
					String uetac = imei.length() >= 8 ? imei.substring(0,8) : "";
					String hour = start_t.replace("-", "").replace(" ", "").substring(0, 10);
					String city = "130600";
					String cgi = "";
					if (eci != null && !eci .equals("")) {
						long e = Long.parseLong(eci);
						String e1 = String.valueOf((int)(e / 256));
						String e2 = String.valueOf((int)(e % 256));
						cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
					}
					String arr1 = data +
							StringUtils.DELIMITER_VERTICAL + uetac +
							StringUtils.DELIMITER_VERTICAL + cgi +
							StringUtils.DELIMITER_VERTICAL + city +
							StringUtils.DELIMITER_VERTICAL + hour;

					mos.write("s1ugameorgn",  NullWritable.get(), new Text(arr1),"D:/classdata/output2/tb_xdr_s1u_game/p_city="+city+"/p_hour="+hour+"/s1u_other_"+city);
				}else if (tableName.contains(TablesConstants.FILTER_STREAMING)){
					String start_t = datas[1];
					String imei = datas[5];
					String eci = datas[9];
					String mcc = datas[24];
					String mnc = datas[25];
					String uetac = imei.length() >= 8 ? imei.substring(0,8) : "";
					String hour = start_t.replace("-", "").replace(" ", "").substring(0, 10);
					String city = "130600";
					String cgi = "";
					if (eci != null && !eci .equals("")) {
						long e = Long.parseLong(eci);
						String e1 = String.valueOf((int)(e / 256));
						String e2 = String.valueOf((int)(e % 256));
						cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
					}
					String arr1 = data +
							StringUtils.DELIMITER_VERTICAL + uetac +
							StringUtils.DELIMITER_VERTICAL + cgi +
							StringUtils.DELIMITER_VERTICAL + city +
							StringUtils.DELIMITER_VERTICAL + hour;

					mos.write("s1ustreamingorgn",  NullWritable.get(), new Text(arr1),"D:/classdata/output2/tb_xdr_s1u_streaming/p_city="+city+"/p_hour="+hour+"/s1u_other_"+city);
				}else if (tableName.contains(TablesConstants.FILTER_DNS)){
					String start_t = datas[1];
					String imei = datas[5];
					String eci = datas[13];
//					String mcc = datas[24];
//					String mnc = datas[25];
					String uetac = imei.length() >= 8 ? imei.substring(0,8) : "";
					String hour = start_t.replace("-", "").replace(" ", "").substring(0, 10);
					String city = "130600";
					String cgi = "";
//					if (eci != null && !eci .equals("")) {
//						long e = Long.parseLong(eci);
//						String e1 = String.valueOf((int)(e / 256));
//						String e2 = String.valueOf((int)(e % 256));
//						cgi = mcc + "-" + mnc + "-" + e1 + "-" + e2;
//					}
					String arr1 = data +
							StringUtils.DELIMITER_VERTICAL + uetac +
							StringUtils.DELIMITER_VERTICAL + cgi +
							StringUtils.DELIMITER_VERTICAL + city +
							StringUtils.DELIMITER_VERTICAL + hour;

					mos.write("s1udnsorgn",  NullWritable.get(), new Text(arr1),"D:/classdata/output2/tb_xdr_s1u_dns/p_city="+city+"/p_hour="+hour+"/s1u_other_"+city);
				}
			}

		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}
