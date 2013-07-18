package mp2;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;
import java.util.UUID;
 
/**
 * Generates logs to be used by the log querier's grep functionality
 */
public class LogGen {
	
        private final static String rare = "purple";
        private final static String medium = "blue";
        private final static String common = "white";
       
        private File f;
        private BufferedWriter bw;
       
        public LogGen(String fileName) throws IOException {
                f = new File(fileName);
                
                if (!f.getParentFile().exists())
                	f.getParentFile().mkdirs();
                
                bw = new BufferedWriter(new FileWriter(f));
        }
       
        /**
         * Creates a log file of approximately size bytes
         * @param size size of the file in bytes (not exact, but very close)
         * @throws IOException
         */
        public LogInfo genFile(int size) throws IOException {
                int i = 1;
                
                int fileSize = 100*1024*1024*size;  //create logs of size 100MB, 200MB, 300MB
                
                //int fileSize = 250*1024*1024*size; // create logs of length 250MB, 500MB, 750MB, 1GB
                
                int rareIdx = 0;
                int mediumIdx = 0;
                int commonIdx = 0;
                
                // writes preset strings every 1000, 100, and 10 lines
                // could be changed for more variation/randomness
                while (f.length() < fileSize) {
                        if (i % 1000 == 0) {
                                bw.write(rare + '\n');
                                i++;
                                rareIdx++;
                                continue;
                        }      
                       
                        if (i % 100 == 0) {
                                bw.write(medium + '\n');
                                i++;
                                mediumIdx++;
                                continue;
                        }
                       
                        if (i % 10 == 0) {
                                bw.write(common + '\n');
                                i++;
                                commonIdx++;
                                continue;
                        }                                      
                       
                        // writes a random string
                        bw.write(UUID.randomUUID().toString());
                        bw.write('\n');
                        i++;
                } 
                
                bw.close();
                
                return new LogInfo(rare, medium, common, rareIdx, mediumIdx, commonIdx);
        }
       
        /**
         * Returns associated file length in bytes; used in testing
         * @return file length in bytes
         * @throws IOException
         */
        public long getFileSize() throws IOException {
                return f.length();
        }
        

        public static void main(String[] args) throws IOException {
               
        	int index = Integer.parseInt(args[0]);
        			
        	LogGen log= new LogGen("machine." + index + ".log");
        	log.genFile(index);
        }
       
}