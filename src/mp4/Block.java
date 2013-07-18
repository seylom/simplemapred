package mp4;
/**
 * A block representing a part of a file
 */
public class Block {
	private String ownerFileName;
	private int index;	
	private String blockName;
		
	/**
	 * @param filename
	 * @param blockIndex
	 * @param blockData
	 */
	public Block(String ownerfilename,int blockIndex,String blockName){
		ownerFileName = ownerfilename;
		index = blockIndex;
		this.blockName = blockName;
	}
	
	/**
	 * @return
	 */
	public String getName(){
		return this.ownerFileName;
	}
	
	/**
	 * @return
	 */
	public int getIndex(){
		return this.index;
	}
	
	/**
	 * @return
	 */
	public String getBlockName(){
		return this.blockName;
	}
}
