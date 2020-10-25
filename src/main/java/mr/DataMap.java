package mr;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DataMap extends Mapper<LongWritable, Text, Text, Text> {

	private final Text key = new Text();
	@Override
	public void map(LongWritable keys, Text value, Context context)
			throws IOException, InterruptedException {

		if (value.getLength() > 0) {
			String filePath = context.getInputSplit().toString().toUpperCase();
//			System.out.println(filePath);
			String[] values = value.toString().split(StringUtils.DELIMITER_INNER_ITEM);


			if (filePath.contains("S1MME") && values.length > 1) {
				if (StringUtils.isNotEmpty(values[3])){
					key.set(values[3]);
					context.write(key, new Text(value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_S1MME));
				}
			}else if (filePath.contains("S1UHTTP") && values.length > 1) {
					if (StringUtils.isNotEmpty(values[3])){
						key.set(values[3]);
						context.write(key, new Text( value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_HTTP));
					}
			}else if (filePath.contains("S1UOTHERS") && values.length > 1) {
				if (StringUtils.isNotEmpty(values[3])){
					key.set(values[3]);
					context.write(key, new Text( value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_OTHER));
				}
			}else if (filePath.contains("S1UIM") && values.length > 1) {
				if (StringUtils.isNotEmpty(values[3])){
					key.set(values[3]);
					context.write(key, new Text( value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_IM));
				}
			}else if (filePath.contains("S1UGAME") && values.length > 1) {
				if (StringUtils.isNotEmpty(values[3])){
					key.set(values[3]);
					context.write(key, new Text( value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_GAME));
				}
			}else if (filePath.contains("S1USTREAMING") && values.length > 1) {
				if (StringUtils.isNotEmpty(values[3])){
					key.set(values[3]);
					context.write(key, new Text( value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_STREAMING));
				}
			}else if (filePath.contains("S1UDNS") && values.length > 1) {
				if (StringUtils.isNotEmpty(values[4])){
					key.set(values[4]);
					context.write(key, new Text( value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_DNS ));
				}
			}
		}

	}



}
