package io.arabesque.utils;

/**
 * Suns's code, to avoid 8 and guava library
 * Created by siganos on 5/7/16.
 */
public class Hashing {
    public static int murmur3_32(int[] var0) {
        return murmur3_32(0, (int[])var0, 0, var0.length);
    }

    public static int murmur3_32(int var0, int[] var1) {
        return murmur3_32(var0, (int[])var1, 0, var1.length);
    }

    public static int murmur3_32(int var0, int[] var1, int var2, int var3) {
        int var4 = var0;
        int var5 = var2;

        for(int var6 = var2 + var3; var5 < var6; var4 = var4 * 5 + -430675100) {
            int var7 = var1[var5++];
            var7 *= -862048943;
            var7 = Integer.rotateLeft(var7, 15);
            var7 *= 461845907;
            var4 ^= var7;
            var4 = Integer.rotateLeft(var4, 13);
        }

        var4 ^= var3 * 4;
        var4 ^= var4 >>> 16;
        var4 *= -2048144789;
        var4 ^= var4 >>> 13;
        var4 *= -1028477387;
        var4 ^= var4 >>> 16;
        return var4;
    }

}
