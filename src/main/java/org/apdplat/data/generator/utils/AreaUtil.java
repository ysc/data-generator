package org.apdplat.data.generator.utils;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ysc on 19/04/2018.
 */
public class AreaUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(AreaUtil.class);

    private static final Map<String, String> CITYS_LAT = new HashMap<>();
    private static final Map<String, String> CITYS_LONG = new HashMap<>();

    static {
        for(String line : MultiResourcesUtils.load("area_info.txt")){
            //id    city_id city    city_full   province_id province    province_full
            String[] attr = line.split("_");
            if(attr == null || attr.length != 7){
                LOGGER.error("错误的区域数据: {}", line);
            }else{
                String id = attr[0];
                String city = attr[2];
                String city_full = attr[3];
                String province = attr[5];
                String province_full = attr[6];
                CITYS_LAT.put(city, null);
                CITYS_LONG.put(city, null);
            }
        }

        for(String line : MultiResourcesUtils.load("geo.txt")){
            String[] attr = line.split("\\s+");
            if(attr == null || attr.length != 4){
                LOGGER.error("错误的经纬度数据: {}", line);
            }else{
                String city = attr[1];
                String latitude = attr[2];
                String longitude = attr[3];
                if(city.endsWith("市")){
                    city = city.substring(0, city.length()-1);
                }
                if(CITYS_LAT.containsKey(city)){
                    CITYS_LAT.put(city, latitude.replace("北纬", ""));
                    CITYS_LONG.put(city, longitude.replace("东经", ""));
                }
            }
        }
    }

    public static Float getLongitude(String city){
        String value = CITYS_LONG.get(city);
        if(value != null){
            try {
                return Float.parseFloat(value);
            }catch (Exception e){
                //
            }
        }
        return null;
    }

    public static Float getLatitude(String city){
        String value = CITYS_LAT.get(city);
        if(value != null){
            try {
                return Float.parseFloat(value);
            }catch (Exception e){
                //
            }
        }
        return null;
    }

    public static void main(String[] args) {
        System.out.println(JSON.toJSONString(CITYS_LAT, true));
        System.out.println(JSON.toJSONString(CITYS_LONG, true));
        CITYS_LAT.entrySet().forEach(entry->{
            if(entry.getValue() == null){
                System.err.println("latitude: "+entry.getKey());
            }
        });
        CITYS_LONG.entrySet().forEach(entry->{
            if(entry.getValue() == null){
                System.err.println("longitude: "+entry.getKey());
            }
        });
    }
}
