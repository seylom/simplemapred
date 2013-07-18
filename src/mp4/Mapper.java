package mp4;

public class Mapper {	
	
	/* Maximum number of nodes for hashing purposes */
	public static final int MAX_NODES = 1000;
	
	/* This number represents the number of nodes in the system
	   and is only useful for testing. In regular use, this number 
 	   will be passed in as a parameter to getNodeNumber.      */
	public static final int NUM_NODES = 3;		
	

	/** Class entry point - used for testing **/
	public static void main(String[] args) {
		for (int i = 0; i < args.length; i++) {
			getNodeNumber(args[i], NUM_NODES);
		}		
	}
	
	/**
	 * Get a node number in the range [1, numNodes] from a hashing
	 * function on the file name.
	 * 
	 * @param filename name of file
	 * @param numNodes number of nodes in the system
	 * 
	 * @return number of the node responsible for the file
	 */
	public static int getNodeNumber(String filename, int numNodes) {
		char[] c = filename.toCharArray();
		int total = 0;
		
		for (int i = 0; i < c.length; i++) {
			total += c[i];
		}
		
		System.out.println("\'" + filename + "\' (hash id " +  total % MAX_NODES + 
				") mapped to node " + findBucket(total % MAX_NODES, numNodes));
		
		return findBucket(total % MAX_NODES, numNodes);
	}
	
	/**
	 * Helper function to getNodeNumber.  
	 */
	private static int findBucket(int n, int numNodes) {		
		for (int i = 1; i <= numNodes; i++) {
			if (n < (MAX_NODES / numNodes * i))
				return i;
		}
		
		return numNodes;
	}
}
