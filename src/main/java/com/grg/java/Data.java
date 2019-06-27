package com.grg.java;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Data implements Serializable {
    public int getA() {
        return a;
    }

    public void setA(int a) {
        this.a = a;
    }

    public int getB() {
        return b;
    }

    public void setB(int b) {
        this.b = b;
    }

    public int getC() {
        return c;
    }

    public void setC(int c) {
        this.c = c;
    }

    int  a;
    int  b;
    int c;


}
