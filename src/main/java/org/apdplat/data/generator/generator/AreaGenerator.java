package org.apdplat.data.generator.generator;

import org.apdplat.data.generator.mysql.MySQLUtils;
import org.apdplat.data.generator.utils.AreaUtil;
import org.apdplat.data.generator.utils.MultiResourcesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by ysc on 18/04/2018.
 */
public class AreaGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(AreaGenerator.class);

    public static void clear(){
        MySQLUtils.clean("area");
    }
    public static int generate(){
        Connection con = MySQLUtils.getConnection();
        if(con == null){
            return 0;
        }
        int count = 0;
        PreparedStatement pst = null;
        ResultSet rs = null;
        String sql = "insert into area (id, city, city_full, province, province_full, longitude, latitude) values(?, ?, ?, ?, ?, ?, ?);";
        try {
            con.setAutoCommit(false);
            pst = con.prepareStatement(sql);
            for(String line : MultiResourcesUtils.load("area_info.txt")){
                //id    city_id city    city_full   province_id province    province_full
                String[] attr = line.split("_");
                if(attr == null || attr.length != 7){
                    LOGGER.error("错误的区域数据: {}", line);
                }else{
                    String city = attr[2];
                    String city_full = attr[3];
                    String province = attr[5];
                    String province_full = attr[6];
                    Float longitude = AreaUtil.getLongitude(city);
                    Float latitude = AreaUtil.getLatitude(city);
                    if(longitude == null || latitude == null){
                        continue;
                    }
                    count++;
                    pst.setInt(1, count);
                    pst.setString(2, city);
                    pst.setString(3, city_full);
                    pst.setString(4, province);
                    pst.setString(5, province_full);
                    pst.setFloat(6, longitude);
                    pst.setFloat(7, latitude);
                    pst.addBatch();
                }
            }
            pst.executeBatch();
            con.commit();
            LOGGER.info("保存到数据库成功");
        } catch (Exception e) {
            LOGGER.error("保存到数据库失败", e);
        } finally {
            MySQLUtils.close(con, pst, rs);
        }
        return count;
    }

    public static void main(String[] args) {
        clear();
        generate();
    }
}
