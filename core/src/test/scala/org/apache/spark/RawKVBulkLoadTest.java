package org.apache.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class RawKVBulkLoadTest {
    String prefix = "test01";
    int keyCount  = 10000 ;//1000000
    int valueLength= 64;
    int ingestNumber = 1;

    private TaskAttemptContext context;
    private String OUT_PUT = "/tmp/output";
    private FileSystem localfileSystem;
    private String OUTPUT_TABLE_NAME ="";


    private String  genKey(int i ){
        String s = "";

        if(i < 10) {
            s = s + "0000000";
        } else if(i < 100) {
            s = s + "000000";
        } else if(i < 1000) {
            s = s + "00000";
        } else if(i < 10000) {
            s = s + "0000";
        } else if(i < 100000) {
            s = s + "000";
        } else if(i < 1000000) {
            s = s + "00";
        }else if(i < 10000000) {
            s = s + "0";
        }
        return  s + i;
    }

    private String genValue() {
        String s = "";
        for(int i=1;i<=valueLength;i++){
            s = s + "A";
        }
        return s;
    }

    @Before
    public void setup() {
        JobConf jobConf = new JobConf();
        TaskAttemptID taskAttemptID = new TaskAttemptID();
        context = new TaskAttemptContextImpl(jobConf, taskAttemptID);
        context.getConfiguration().set(OUTPUT_TABLE_NAME, "test");
        localfileSystem = new RawLocalFileSystem();
        localfileSystem.setConf(new Configuration());
    }

    @Test
//    public void testGetFullTableName() {
//        Assert.assertEquals(ShareStoreRecordWriter.getFullTableName(context), "test00000");
//    }

    @Test
    public void testWriter() throws IOException, InterruptedException {

    }

    @After
    public void clean() throws IOException {
        localfileSystem.delete(new Path(OUT_PUT));
    }
}
