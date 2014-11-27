package com.vip.mlk.hdfs;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

public class MapFileLookup {
    
    private static MapFileLookup instance;
    private Configuration conf = new Configuration();
    private FileSystem fs = null;
    private MapFile.Reader reader = null;
    
    
    
    @SuppressWarnings("deprecation")
    private MapFileLookup(String fileName){   
        try {
            fs = FileSystem.get(conf);
 
            try {
                reader = new MapFile.Reader(fs, fileName, conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
 
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }
    
    
    public static MapFileLookup getInstance(String fileName)
    {
        if(null == instance)
        {
            synchronized(MapFileLookup.class)
            {
                if(null == instance)
                {
                    instance = new MapFileLookup(fileName);                
                }
            }
        }
        return instance;
    }

    public void update(String fileName)
    {
        close();
        synchronized(MapFileLookup.class)
        {
            if(null == instance)
            {
                instance = new MapFileLookup(fileName);                
            }
        }
    }
    
    public String getValue(String key)
    {
        Text txtValue = new Text();
        try {
            reader.get(new Text(key), txtValue);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return txtValue.toString();
    }
    
    
    public void close()
    {
        if(reader != null)
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }
    
    
    
    public static void  main(String[] args) throws IOException {
 
    }
}
