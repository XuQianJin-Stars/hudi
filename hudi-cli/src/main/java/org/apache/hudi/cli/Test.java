package org.apache.hudi.cli;

import java.util.Arrays;
import java.util.List;

public class Test {
  public static void main(String[] args) {
    String str1 = "fa=:a,b,c";
    String str2 = "fa:a,b,c";
    String str3 = "fa=1:a,";
    System.out.println(Arrays.asList(str3.split("=|:|,")));

  }
  public List<String> split(String str) {
    return null;
  }
}
