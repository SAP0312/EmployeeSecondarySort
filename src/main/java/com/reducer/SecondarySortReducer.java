package com.reducer;

import com.utils.CompositeKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondarySortReducer extends Reducer<CompositeKey, NullWritable,CompositeKey,NullWritable> {

    public void reduce(CompositeKey key,Iterable<NullWritable> values,Context context) throws IOException,InterruptedException{
        for(NullWritable value:values){
            context.write(key,NullWritable.get());
        }
    }
}
