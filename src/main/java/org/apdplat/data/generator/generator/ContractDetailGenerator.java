package org.apdplat.data.generator.generator;

import org.apdplat.data.generator.mysql.MySQLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * Created by ysc on 18/04/2018.
 */
public class ContractDetailGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContractDetailGenerator.class);

    public static void clear(){
        MySQLUtils.clean("contract_detail");
    }

    public static void generate(int contractCount, int contractDetailLimit, int itemQuantityLimit, Map<Integer, Float> items, List<String> dayStrs, int batchSize){
        Connection con = MySQLUtils.getConnection();
        if(con == null){
            return;
        }
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            con.setAutoCommit(false);
            String sql = "insert into contract_detail (id, item_id, item_quantity, price, contract_id, sign_day) values(?, ?, ?, ?, ?, ?);";
            pst = con.prepareStatement(sql);
            int contractDetailId = 1;
            for(int j=1; j<contractCount; j++) {
                int contractId = j;
                float totalPrice = 0;
                int len = new Random(System.nanoTime()).nextInt(contractDetailLimit)+1;
                Set<Integer> itemUsed = new HashSet<>();
                for (int i = 0; i < len; i++) {
                    int r = new Random(System.nanoTime()).nextInt(dayStrs.size());
                    String dayStr = dayStrs.get(r);
                    int item_id = new Random(System.nanoTime()).nextInt(items.size())+1;
                    while(itemUsed.contains(item_id)){
                        item_id = new Random(System.nanoTime()).nextInt(items.size())+1;
                    }
                    itemUsed.add(item_id);
                    int item_quantity = new Random(System.nanoTime()).nextInt(itemQuantityLimit) + 1;
                    float price = items.get(item_id) * item_quantity;
                    totalPrice += price;
                    pst.setInt(1, contractDetailId++);
                    pst.setInt(2, item_id);
                    pst.setInt(3, item_quantity);
                    pst.setFloat(4, price);
                    pst.setInt(5, contractId);
                    pst.setString(6, dayStr);
                    pst.addBatch();

                    if ((i + 1) % batchSize == 0) {
                        pst.executeBatch();
                    }
                }
                updateContractPrice(con, contractId, totalPrice);
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

    private static void updateContractPrice(Connection con, int contractId, float totalPrice) {
        try{
            con.prepareStatement("update contract set contract_price="+totalPrice+" where id="+contractId).executeUpdate();
        }catch (Exception e){
            LOGGER.error("更新合同总价失败", e);
        }
    }
}
