package com.utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortCompKeyComparator extends WritableComparator {
    protected SecondarySortCompKeyComparator() {

        super(CompositeKey.class);
    }
        @Override
                public int compare(WritableComparable w1,WritableComparable w2) {
                    CompositeKey key1 = (CompositeKey) w1;
                    CompositeKey key2 = (CompositeKey) w2;

                    int cmpresult = key1.getDeptNo().compareTo(key2.getDeptNo());
                    if(cmpresult==0){
                         return -key1.getNameEmpIDPair().compareTo(key2.getNameEmpIDPair());
                    }
                    return cmpresult;

        }
    }


