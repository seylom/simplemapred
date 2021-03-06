package mp5;

import java.io.File;
import java.io.IOException; 
import mp4.SdfsClient;
import mp4.SdfsNode;

/**
 *
 */
public class TestMapleJuice {

    public void test_jar_with_processbuilder(){

        String exe = "/tmp/ayivigu2_kjustic3/localhost-10000/maplejuice/maple_exe.jar";
        String prefix = "mj";
        String targetfile = "/tmp/ayivigu2_kjustic3/localhost-10000/sdfs/sdfstestfile.part0";

        String workingPath = "/tmp/ayivigu2_kjustic3/localhost-10000/maplejuice/";

        ProcessBuilder pb = new ProcessBuilder("java", "-jar", exe , targetfile, prefix);
        pb.directory(new File(workingPath));
        //pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        try {
            Process pc = pb.start();
            pc.waitFor();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void test_maple_execution_from_sdfs_storage() throws InterruptedException {
       (new Thread() {
            @Override
            public void run() {
                new SdfsNode("localhost", 10000, "localhost", 10000, true,5000,true);
            }
        }).start();

        Thread.sleep(1000);

        for (int i = 0; i < 6; i++) {
            final int idx = i;
            (new Thread() {
                @Override
                public void run() {
                    new SdfsNode("localhost", 7770 + idx);
                }
            }).start();

            Thread.sleep(100);
        }

        Thread.sleep(1000);

        // initiate a transfer
        SdfsClient client = new SdfsClient("localhost", 10000);

        client.put("c:/mj/simplefile1",
                "sdfstestfile1");

        client.put("c:/mj/simplefile2",
                "sdfstestfile2");

        client.put("c:/mj/simplefile3",
                "sdfstestfile3");

        Thread.sleep(3000);

        client.maple(new String[]{"maple","maple_exe.jar","mj","sdfstestfile1","sdfstestfile2","sdfstestfile3"});

        Thread.sleep(10000);

        client.juice(new String[]{"juice","juice_exe.jar","3","mj","destination_sdfsfile"});

        Thread.sleep(10000);

        client.get("destination_sdfsfile");
    }

    public void test_maple_execution_from_sdfs_storage_no_crash() throws InterruptedException {



        (new Thread() {
            @Override
            public void run() {
                new SdfsNode("localhost", 10000, "localhost", 10000, true);
            }
        }).start();

        Thread.sleep(1000);

        for (int i = 0; i < 1; i++) {
            final int idx = i;
            (new Thread() {
                @Override
                public void run() {
                    new SdfsNode("localhost", 7770 + idx);
                }
            }).start();

            Thread.sleep(100);
        }

        Thread.sleep(1000);

        // initiate a transfer
        SdfsClient client = new SdfsClient("localhost", 10000);

        client.put("simplefile1",
                "sdfstestfile1");

        client.put("simplefile2",
                "sdfstestfile2");

        client.put("simplefile3",
                "sdfstestfile3");

        Thread.sleep(3000);

        client.maple(new String[]{"maple","maple_exe.jar","mj","sdfstestfile1","sdfstestfile2","sdfstestfile3"});

        Thread.sleep(10000);

        client.juice(new String[]{"juice","juice_exe.jar","10","mj","destination_sdfsfile"});

        //Thread.sleep(10000);

        //client.get("destination_sdfsfile");
    }


    public void test_with_sdfsClient(){

    }

    /**
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        TestMapleJuice test = new TestMapleJuice();
        test.test_maple_execution_from_sdfs_storage_no_crash();
        //test.test_maple();
        //test.test_jar_with_processbuilder();
    }
}
