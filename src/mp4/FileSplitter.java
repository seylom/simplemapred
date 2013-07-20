package mp4;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;


public class FileSplitter {

	/** Entry point for testing **/
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Usage: FileSplitter <file> <pieces>");
			return;
		}
		
		File target = new File(args[0]);
		split(target, Integer.parseInt(args[1]));		
	}
	
	/**
	 * Splits FILE into SHARDS pieces. All shards will have the same length
	 * except the last one, which may be shorter.
	 * @param file		file to be split
	 * @param l	number of pieces to split the file into
	 * @return			array list containing the sharded files
	 */
	public static ArrayList<File> split(File file, long l) {
		ArrayList<File> ret = new ArrayList<File>();
		long fileSize = file.length();
		
		if (fileSize == 0) {
			System.out.println("File read error");
			return null;
		}
		
		if (l <= 0) {
			ret.add(file);
			return ret;
		}
		
		// rounds up the size of the shard
		int shardSize = (int) (Math.ceil((double) fileSize / l));
		int readLength = shardSize;
		int parts = 0;
		byte[] buffer;
		File filePart;
		FileInputStream fis;
		FileOutputStream fos;
		String partName;
		
		try {
			fis = new FileInputStream(file);
			
			while (fileSize > 0) {	
				if (fileSize < shardSize) {
					readLength = (int) fileSize;
				}
				buffer = new byte[readLength];				
				fileSize -= fis.read(buffer, 0, readLength);
				partName = file + ".part" + (++parts);
				filePart = new File(partName);
				fos = new FileOutputStream(filePart);
				fos.write(buffer);
				fos.flush();
				fos.close();

				ret.add(filePart);
			}
			
			fis.close();
		}
		catch (IOException e) {
			System.out.println("File splitting error.");
			return null;
		}
		
		return ret;
	}
	
	
	/**
	 * @param file
	 * @param shardSize
	 * @return
	 */
	public static ArrayList<File> splitByShardSize(File file, int shardSize) {	
		if (!file.isFile() || !file.exists()) {
			System.out.println("File does not exist.");
			return null;
		}
		
		ArrayList<File> ret = new ArrayList<File>();
		
		long fileSize = file.length();
		
		if (fileSize == 0) {
			ret.add(file);
			return ret;
		}
		
		
		
		if (shardSize <= 0) {
			System.out.println("shard size must be positive");
			return null;
		}
		
		if (file.length() <= shardSize){
			ret.add(file);
			return ret;
		}
		
		int readLength = shardSize;
		int parts = 0;
		byte[] buffer;
		File filePart;
		FileInputStream fis;
		FileOutputStream fos;
		String partName;
		
		try {
			fis = new FileInputStream(file);
			
			while (fileSize > 0) {	
				if (fileSize < shardSize) {
					readLength = (int) fileSize;
				}
				buffer = new byte[readLength];				
				fileSize -= fis.read(buffer, 0, readLength);
				partName = file + ".part" + ++parts;
				filePart = new File(partName);
				fos = new FileOutputStream(filePart);
				fos.write(buffer);
				fos.flush();
				fos.close();

				ret.add(filePart);
			}
			
			fis.close();
		}
		catch (IOException e) {
			System.out.println("File splitting error.");
			return null;
		}
		
		return ret;
	}
}
