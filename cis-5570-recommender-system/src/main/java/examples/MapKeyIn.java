package examples;

public class MapKeyIn {
	private Boolean matrixBitIndicator = null;
	private Integer maxI = 0;
	private Integer maxK = 0;
	private Integer currentI = 0;
	private Integer currentJ = 0;
	
	public static MapKeyIn createMKey(Integer currentI, Integer maxK) {
		MapKeyIn key = new MapKeyIn();
		key.matrixBitIndicator = true;
		key.currentI = currentI;
		key.maxK = maxK;
		return key;
	}
	
	public static MapKeyIn createNKey(Integer maxI, Integer currentJ) {
		MapKeyIn key = new MapKeyIn();
		key.matrixBitIndicator = false;
		key.maxI = maxI;
		key.currentJ = currentJ;
		return key;
	}

	public Boolean getMatrixBitIndicator() {
		return matrixBitIndicator;
	}

	public Integer getMaxI() {
		return maxI;
	}

	public Integer getMaxK() {
		return maxK;
	}

	public Integer getCurrentI() {
		return currentI;
	}

	public Integer getCurrentJ() {
		return currentJ;
	}
}
