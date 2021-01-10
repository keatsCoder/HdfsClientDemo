package cn.keats;

import java.nio.charset.StandardCharsets;

public class Test {
    public static void main(String[] args) {
        byte[] bytes1 = " ".getBytes(StandardCharsets.UTF_8);
        byte[] bytes2 = "\t".getBytes(StandardCharsets.UTF_8);

        System.out.println(bytes1.equals(bytes2));
    }
}
