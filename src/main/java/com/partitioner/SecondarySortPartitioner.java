package com.partitioner;

import com.utils.CompositeKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner extends Partitioner<CompositeKey, NullWritable> {

    @Override
    public int getPartition(CompositeKey key,NullWritable value,int numreduceTasks){

        return (key.getDeptNo().hashCode()%numreduceTasks);

    }
}
