package com.cw.flink;

import org.apache.flink.api.java.utils.ParameterTool;

public class Test {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String yt = parameterTool.get("yt");
        System.out.println("yt = " + yt);
    }
}
