package com.alibaba.datax.plugin.reader.mongodbreader.util;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;


public class AESUtil {

    private static final org.apache.commons.codec.binary.Base64 BASE64 = new org.apache.commons.codec.binary.Base64();

    private static final String AES = "AES";

    /**
     * length == 16
     */
//    public static final String AES_KEY = "bzdIOHVJTTJPNXF2NjVsMg==";

    public static final String salt = "!@#$%^&*";

    public static void main(String[] args) {
        String encrypt = encryptString("123213214312","bzdIOHVJTTJPNXF2NjVsMg==");
        System.out.println(encrypt);
        String decrypt = decryptString(encrypt,"bzdIOHVJTTJPNXF2NjVsMg==");
        System.out.println(decrypt);
    }

    public static String encryptString(String ciphertext, String AES_KEY) {
        byte[] keyBytes = Base64.getDecoder().decode(AES_KEY);
        if (keyBytes.length != 16) {
            throw new RuntimeException("Invalid key length");
        }
        byte[] iv = keyBytes;
        String plaintext = null;
        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            SecretKeySpec keySpec = new SecretKeySpec(keyBytes, AES);
            IvParameterSpec ivSpec = new IvParameterSpec(iv);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);
            byte[] cipherBytes = cipher.doFinal(ciphertext.getBytes());
            byte[] plainBytes = Base64.getEncoder().encode(cipherBytes);
            plaintext = new String(plainBytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return plaintext;
    }

    public static String decryptString(String ciphertext, String AES_KEY) {
        byte[] keyBytes = Base64.getDecoder().decode(AES_KEY);
        if (keyBytes.length != 16) {
            throw new RuntimeException("Invalid key length");
        }
        byte[] iv = keyBytes;
        String plaintext;
        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            SecretKeySpec keySpec = new SecretKeySpec(keyBytes, AES);
            IvParameterSpec ivSpec = new IvParameterSpec(iv);
            cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);
            byte[] cipherBytes = Base64.getDecoder().decode(ciphertext);
            byte[] plainBytes = cipher.doFinal(cipherBytes);
            plaintext = new String(plainBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return plaintext;
    }


}
