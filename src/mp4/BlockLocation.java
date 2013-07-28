package mp4;
 
import java.util.HashSet;

public class BlockLocation {

	String filename;
	int blockId;

	HashSet<String> locations = new HashSet<String>();
	
	/**
	 * @param filename
	 * @param blockId
	 */
	public BlockLocation(String filename, int blockId){
		this.filename = filename;
		this.blockId = blockId;
	}
	
	
	/**
	 * @param nodeId
	 */
	public void addLocation(String nodeId){	
		synchronized (locations) {
			if (!locations.contains(nodeId)){
				locations.add(nodeId);
			}
		}
	}
	
	/**
	 * @param nodeId
	 */
	public void removeLocation(String nodeId){	
		synchronized (locations) {
			if (locations.contains(nodeId)){
				locations.remove(nodeId);
			}
		}
	}
}
