package com.mapper;


import com.utils.CompositeKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKey, NullWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
        if(value.toString().length()>0){
            String[] arrEmpAttributes = value.toString().split("\\t");

//        context.write(new CompositeKey(arrEmpAttributes[6].toString(),(arrEmpAttributes[3].toString()+ "\t"
//                                            +arrEmpAttributes[2].toString()+"\t"+ arrEmpAttributes[0].toString()),NullWritable.get());

            context.write(
                    new CompositeKey(
                            arrEmpAttributes[6].toString(),
                            (arrEmpAttributes[3].toString() + "\t"
                                    + arrEmpAttributes[2].toString() + "\t" + arrEmpAttributes[0]
                                    .toString())), NullWritable.get());

        }


    }
}
