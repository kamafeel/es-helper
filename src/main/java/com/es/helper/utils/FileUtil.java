package com.es.helper.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import java.io.*;

/**
 * @author wuzh
 * @version V1.0
 * @Package com.kdom.irs.business.file.util
 * @Description: 文件操作工具类
 * @date 2019-11-30
 */
public class FileUtil {

    public final static Logger logger = LoggerFactory.getLogger(FileUtil.class);

    /**
     * 递归删除目录及子目录
     *
     * @param path 目录路径
     * @return
     */
    public static boolean deleteDir(String path) {
        try {
            File file = new File(path);
            if (file.isFile()) {
                file.delete();
            } else {
                File[] files = file.listFiles();
                if (files == null) {
                    file.delete();
                } else {
                    for (File subFile : files) {
                        String absolutePath = subFile.getAbsolutePath();
                        deleteDir(absolutePath);
                    }
                    file.delete();
                }
            }
            return true;
        } catch (Exception e) {
            logger.error("删除目录失败：" + path, e);
            return false;
        }
    }


    /**
     * 根据相对路径获取文件内容
     */
    public static String jsonFileRead(String relativePath) throws Exception {
        ClassPathResource classLoader = new ClassPathResource(relativePath);
        InputStream inputStream = classLoader.getInputStream();
        FileReader fileReader = null;
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
            StringBuffer stringBuffer = new StringBuffer();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuffer.append(line);
            }
            return stringBuffer.toString();
        } catch (Exception e) {
            logger.error("", e);
            throw e;
        } finally {
            if (fileReader != null) {
                fileReader.close();
            }
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }
    }

}
