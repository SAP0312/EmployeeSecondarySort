package com.utils;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements Writable, WritableComparable<CompositeKey> {
    private String deptNo;
    private String NameEmpIDPair;
    public CompositeKey(){

    }
    public CompositeKey(String deptNo,String NameEmpIDPair){
        this.deptNo=deptNo;
        this.NameEmpIDPair=NameEmpIDPair;
    }

    public String toString(){
        return (new StringBuilder().append(deptNo).append("\t").append(NameEmpIDPair).toString());
    }

    public String getDeptNo() {
        return deptNo;
    }

    public void setDeptNo(String deptNo) {
        this.deptNo = deptNo;
    }

    public String getNameEmpIDPair() {
        return NameEmpIDPair;
    }

    public void setNameEmpIDPair(String nameEmpIDPair) {
        NameEmpIDPair = nameEmpIDPair;
    }

    public void readFields(DataInput dataInput) throws IOException{
        deptNo = WritableUtils.readString(dataInput);
        NameEmpIDPair = WritableUtils.readString(dataInput);
    }

    public void write(DataOutput dataOutput) throws IOException{
        WritableUtils.writeString(dataOutput,deptNo);
        WritableUtils.writeString(dataOutput,NameEmpIDPair);
    }
    public int compareTo(CompositeKey objeKeyPair){
        int result = deptNo.compareTo(objeKeyPair.deptNo);
        if(result==0){
            return NameEmpIDPair.compareTo(objeKeyPair.NameEmpIDPair);
        }
        return result;
    }


}
