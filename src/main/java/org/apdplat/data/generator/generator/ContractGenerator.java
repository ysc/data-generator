package org.apdplat.data.generator.generator;

import org.apdplat.data.generator.mysql.MySQLUtils;
import org.apdplat.data.generator.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by ysc on 18/04/2018.
 */
public class ContractGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ContractGenerator.class);

    private static final List<String> STATES = Arrays.asList("新建", "签订", "生效", "履行中", "终止", "作废");

    public static void clear(){
        MySQLUtils.clean("contract");
    }

    public static void generate(int contractCount, List<String> dayStrs, int customerCount, int salesStaffCount, int batchSize){
        generate(0, contractCount, dayStrs, customerCount, salesStaffCount, batchSize);
    }

    public static void generate(int start, int contractCount, List<String> dayStrs, int customerCount, int salesStaffCount, int batchSize){
        Connection con = MySQLUtils.getConnection();
        if(con == null){
            return ;
        }
        PreparedStatement pst = null;
        ResultSet rs = null;
        String sql = "insert into contract (id, contract_price, state, sign_day, sales_staff_id, customer_id) values(?, ?, ?, ?, ?, ?);";
        try {
            con.setAutoCommit(false);
            pst = con.prepareStatement(sql);
            for(int i=start; i<contractCount; i++){
                int r = new Random(System.nanoTime()).nextInt(dayStrs.size());
                String dayStr = dayStrs.get(r);
                r = new Random(System.nanoTime()).nextInt(STATES.size());
                String state = STATES.get(r);
                int sales_staff_id = new Random(System.nanoTime()).nextInt(salesStaffCount)+1;
                int customer_id = new Random(System.nanoTime()).nextInt(customerCount)+1;
                int contractId = i+1;
                pst.setInt(1, contractId);
                pst.setFloat(2, 0);
                pst.setString(3, state);
                pst.setString(4, dayStr);
                pst.setInt(5, sales_staff_id);
                pst.setInt(6, customer_id);
                pst.addBatch();

                if((i+1) % batchSize == 0) {
                    pst.executeBatch();
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
    }

    private static List<String> getDayStrs() {
        List<String> dayStrs = new ArrayList<>();
        Connection con = MySQLUtils.getConnection();
        if(con == null){
            return dayStrs;
        }
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            String sql = "select day_str from day_dimension";
            LOGGER.info("开始查询, SQL: {}", sql);
            pst = con.prepareStatement(sql);
            rs = pst.executeQuery();
            LOGGER.info("查询结束, 开始处理数据");
            while (rs.next()) {
                String day_str = rs.getString("day_str");
                dayStrs.add(day_str);
            }
        } catch (Exception e) {
            LOGGER.error("查询失败", e);
        } finally {
            MySQLUtils.close(con, pst, rs);
        }
        return dayStrs;
    }

    public static void main(String[] args) {
        //合同数
        int contractCount = Config.getIntValue("contractCount") == -1 ? 20000 : Config.getIntValue("contractCount");
        List<String> dayStrs = getDayStrs();
        int customerCount = MySQLUtils.getCount("customer");
        int salesStaffCount = MySQLUtils.getCount("sales_staff");
        int batchSize = Config.getIntValue("batchSize") == -1 ? 1000 : Config.getIntValue("batchSize");
        ContractGenerator.generate(10000, contractCount, dayStrs, customerCount, salesStaffCount, batchSize);
    }
}
