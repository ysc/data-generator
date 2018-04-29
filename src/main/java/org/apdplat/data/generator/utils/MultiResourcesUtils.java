package org.apdplat.data.generator.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * Created by ysc on 18/04/2018.
 */
public class MultiResourcesUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiResourcesUtils.class);

    public static List<String> load(String resourceName) {
        List<String> list = new ArrayList<>();
        try {
            Enumeration<URL> ps = Thread.currentThread().getContextClassLoader().getResources(resourceName);
            List<String> urls = new ArrayList<>();
            while (ps.hasMoreElements()) {
                URL url = ps.nextElement();
                urls.add(url.toString());
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(url.openStream()));
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    line = line.trim();
                    if (StringUtils.isBlank(line)) {
                        continue;
                    }
                    if (line.startsWith("#") || line.startsWith("//")) {
                        continue;
                    }
                    list.add(line);
                }
            }
            LOGGER.info("加载资源: {}", urls);
            LOGGER.info("成功加载的资源行数: {}", list.size());
        } catch (Exception e) {
            LOGGER.error("加载资源出错", e);
        }
        return list;
    }
}
