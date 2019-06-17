package com.grg.java;

import java.io.Serializable;
@SuppressWarnings("serial")
public class Student implements Serializable {
    String sid;
    String sname;
    int sage;
    public String getSid() {
        return sid;
    }
    public void setSid(String sid) {
        this.sid = sid;
    }
    public String getSname() {
        return sname;
    }
    public void setSname(String sname) {
        this.sname = sname;
    }
    public int getSage() {
        return sage;
    }
    public void setSage(int sage) {
        this.sage = sage;
    }
    @Override
    public String toString() {
        return "Student [sid=" + sid + ", sname=" + sname + ", sage=" + sage + "]";
    }

}
