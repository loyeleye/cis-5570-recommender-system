package examples;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;


public class MapValueOut {
	boolean failedLoad = false;
	Boolean matrixBitIndicator;
	Integer jValue;
	Integer cellValue;
	
	public MapValueOut(String textValue) {
		// M,j,m_ij or N,j,n_jk
		String[] values = StringUtils.split(textValue, ",");
		try {
			this.matrixBitIndicator = (values[0] == "M");
			this.jValue = Integer.parseInt(values[1]);
			this.cellValue = Integer.parseInt(values[2]);
		} catch (Exception e) {
			this.matrixBitIndicator = null;
			this.jValue = null;
			this.cellValue = null;
			failedLoad = true;
		}
	}
	
	public MapValueOut(boolean matrixBitIndicator, Integer jValue,
			Integer cellValue) {
		this.matrixBitIndicator = matrixBitIndicator;
		this.jValue = jValue;
		this.cellValue = cellValue;
	}
	
	public boolean hasError() {
		return failedLoad;
	}

	public Boolean getMatrixBitIndicator() {
		return matrixBitIndicator;
	}

	public Integer getjValue() {
		return jValue;
	}

	public Integer getCellValue() {
		return cellValue;
	}
	
	@Override
	public String toString() {
		return (matrixBitIndicator ? "M" : "N") + "," + jValue + "," + cellValue;
	}
	
	public Text toText() {
		return new Text(toString());
	}
}
