package com.es.helper.utils;

import java.util.Base64;
import javax.crypto.Cipher;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashMap;
import java.util.Map;
/**
 * Java RSA 加密工具类
 * 参考： https://blog.csdn.net/qy20115549/article/details/83105736
 */
public class RSAUtils {
    /**
     * 密钥长度 于原文长度对应 以及越长速度越慢
     */
    private final static int KEY_SIZE = 1024;
    /**
     * 用于封装随机产生的公钥与私钥
     */
    private static Map<Integer, String> keyMap = new HashMap<Integer, String>();
    /**
     * 随机生成密钥对
     */
    public static void genKeyPair() throws NoSuchAlgorithmException {
        // KeyPairGenerator类用于生成公钥和私钥对，基于RSA算法生成对象
        KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA");
        // 初始化密钥对生成器
        keyPairGen.initialize(KEY_SIZE, new SecureRandom());
        // 生成一个密钥对，保存在keyPair中
        KeyPair keyPair = keyPairGen.generateKeyPair();
        // 得到私钥
        RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();
        // 得到公钥
        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
        String publicKeyString = Base64.getEncoder().encodeToString(publicKey.getEncoded());
        // 得到私钥字符串
        String privateKeyString = Base64.getEncoder().encodeToString(privateKey.getEncoded());
        // 将公钥和私钥保存到Map
        //0表示公钥
        keyMap.put(0, publicKeyString);
        //1表示私钥
        keyMap.put(1, privateKeyString);
    }
    /**
     * RSA公钥加密
     *
     * @param str       加密字符串
     * @param publicKey 公钥
     * @return 密文
     * @throws Exception 加密过程中的异常信息
     */
    public static String encrypt(String str, String publicKey) throws Exception {
        //base64编码的公钥
        byte[] decoded = Base64.getDecoder().decode(publicKey);
        RSAPublicKey pubKey = (RSAPublicKey) KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(decoded));
        //RSA加密
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.ENCRYPT_MODE, pubKey);
        String outStr = Base64.getEncoder().encodeToString(cipher.doFinal(str.getBytes("UTF-8")));
        return outStr;
    }
    /**
     * RSA私钥解密
     *
     * @param str        加密字符串
     * @param privateKey 私钥
     * @return 明文
     * @throws Exception 解密过程中的异常信息
     */
    public static String decrypt(String str, String privateKey) throws Exception {
        //64位解码加密后的字符串
        byte[] inputByte = Base64.getDecoder().decode(str);
        //base64编码的私钥
        byte[] decoded = Base64.getDecoder().decode(privateKey);
        RSAPrivateKey priKey = (RSAPrivateKey) KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(decoded));
        //RSA解密
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.DECRYPT_MODE, priKey);
        String outStr = new String(cipher.doFinal(inputByte));
        return outStr;
    }
    public static void main(String[] args) throws Exception {
//        long temp = System.currentTimeMillis();
//        //生成公钥和私钥
//        genKeyPair();
//        //加密字符串
//        System.out.println("公钥:" + keyMap.get(0));
//        System.out.println("私钥:" + keyMap.get(1));
//        System.out.println("生成密钥消耗时间:" + (System.currentTimeMillis() - temp) / 1000.0 + "秒");
//        String message = "RSA测试ABCD~!@#$";
//        System.out.println("原文:" + message);
//        temp = System.currentTimeMillis();
        String messageEn = encrypt("dujuan", "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDBxFOtU43iI0G9F0VaOt093G9qY2FaN8BEXfedwZsmxVRjZvp3Wc3wUgNj6Rvtw0cgIG2PhnYlHafWhIgZRSQckjwISzHdiR151CIHPP/lKtIBk4KPrvZRuadJoVTG+O0xgN4UOaVfu0vt/UTcZMjmbVJxWvcvan6hypbPf9d4wwIDAQAB");
      System.out.println(messageEn);
      /**
       * ZHVqdWFu
       * oDS6oOPSnDtdSZB5Gs/XivNZOwRYp1h3RpslSO/Q2v2GnuucYqv3fcTJhkJGnqqJq+HvmRRkzlOM/ZcuX6bf7HVYtDSjeBErOydq2TrUrCUdPJJut35bcFQA12oITkcgtLEbFHCL15IYDjaHNuf4D2xLz5B4KuvXUFPbCTFyD9Q=
       ZHVqdWFu
       iywBKZMEXegndutHrAYPNK/ZJ0J35x9V9ldBnPBOOQgEj+I6K6gna3nmTvF3M4PRX8etlpaZM7Xsd/qvB53d+AnuaHGrXlJ9ULtudstaSo7VOLMIS8rvaUC8ajec7dwFHnZh69h63LZVeE5uSEikcbXTozBeeOuk/kYQQM3TZaw=

       */


//                System.out.println("密文:" + messageEn);
//        System.out.println("加密消耗时间:" + (System.currentTimeMillis() - temp) / 1000.0 + "秒");
//        temp = System.currentTimeMillis();
        String messageDe = decrypt(messageEn, "MIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBAMHEU61TjeIjQb0XRVo63T3cb2pjYVo3wERd953BmybFVGNm+ndZzfBSA2PpG+3DRyAgbY+GdiUdp9aEiBlFJBySPAhLMd2JHXnUIgc8/+Uq0gGTgo+u9lG5p0mhVMb47TGA3hQ5pV+7S+39RNxkyOZtUnFa9y9qfqHKls9/13jDAgMBAAECgYBOHQtusGReacAyrpLy0RAnxBnWVcIFULxd01PjcQ1PD/X5LKaEPtvaqfVb7bmonDSsKuaAGcC/LblfrYYASfNILQV16Vl+GMxMxpEulgEiK+QuVClyhKZ+cv1nIImXZFTI2VYeccGWiBGEOG3NiESQPl3BOVbGO9fxwWp+/FLgYQJBAPQ4u4KdYPqlL9iHAuxQlfQo8hL5jn80lNL14cOTkVR41gYxeZk8hNp8e6K4rKSoL69z2qZ8PCdoiGLeB11lnjUCQQDLHKiJVPd58YKopLH//ga5pDG4+EEeJo65sh8i7oTYnyyPkzgkdaR74fhwdx8kTFnZDwCGR7czIq4lrMNDLXoXAkEAgh374WJCOihqbn24U/m3eyeZmfx2LFXyeNdiGpZz1sKunQwdEkSJL/Mk2BR2fx/QkDU0qIEGd6SdDbfnyp4KhQJAVl5NXHiA8527DbNa7Zw7h91GN315UzTaJCSWEOiUHPkynargiMBtvTAN0OUWnPzKh/5VHsSIJnpsyyB3t60y6QJAVIZW8pJk4SeXBMyPGFUMGowYggN94V2xcsm2jVMM+aRlcT9rJX4BMi42Y/Z3E20qwunEDGJA5w6nMGniJXo3RQ==");
        System.out.println("解密:" + messageDe);
        //System.out.println("解密消耗时间:" + (System.currentTimeMillis() - temp) / 1000.0 + "秒");
    }
}