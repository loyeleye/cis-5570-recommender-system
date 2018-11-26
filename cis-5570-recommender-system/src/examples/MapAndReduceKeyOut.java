package examples;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;


public class MapAndReduceKeyOut {
	Integer iValue = 0;
	Integer kValue = 0;
	
	public MapAndReduceKeyOut(String textValue) {
		// (i,k)
		Pattern number = Pattern.compile("\\d+");
		Matcher m = number.matcher(textValue);
		String i = m.group(1);
		String k = m.group(2);
		
		try {
			this.iValue = Integer.parseInt(i);
			this.kValue = Integer.parseInt(k);
		} catch (NumberFormatException e) {
			iValue = 0;
			kValue = 0;
		}
	}
	
	public MapAndReduceKeyOut(Integer iValue, Integer kValue) {
		this.iValue = iValue;
		this.kValue = kValue;
	}

	public MapAndReduceKeyOut() {
		this.iValue = 0;
		this.kValue = 0;
	}

	public Integer getiValue() {
		return iValue;
	}

	public Integer getkValue() {
		return kValue;
	}
	
	@Override
	public String toString() {
		return "(" + iValue + ", " + kValue + ")";
	}
	
	public Text toText() {
		return new Text(toString());
	}
}
