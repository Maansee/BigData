package udfhive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ToLower extends UDF{

	public Text evaluate(Text text)
	{
		if(text==null) return null;
		String str = text.toString().toLowerCase();
		return new Text(str);
	
		
	}
}
