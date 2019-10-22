import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.constant.GmallConstant;
import com.atguigu.gmall.util.MyKafkaSender;

import java.util.List;

/**
 * @author shkstart
 * @create 2019-10-22 12:50
 */
public class CanalHandle {
    //sql语句操作类型
    private CanalEntry.EventType eventType;
    //表名
    private String tableName;
    //数据结果集
    private List<CanalEntry.RowData> rowDatasList;

    public void handle() {
        if (GmallConstant.DB_ORDER_INFO.equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType))) {
            sendRowDatas2Kafka(GmallConstant.KAFKA_ORDER_INFO);
        }else if(GmallConstant.DB_USER_INFO.equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType))){
            sendRowDatas2Kafka(GmallConstant.KAFKA_USER_INFO);
        }
    }

    public void sendRowDatas2Kafka(String topic) {
        //插入后所有字段的集合
        for (CanalEntry.RowData rowData : rowDatasList) {
            List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : columnsList) {
                String name = column.getName();
                String value = column.getValue();
                jsonObject.put(name, value);
            }
            MyKafkaSender.send(topic,jsonObject.toJSONString());
        }
    }

    public CanalHandle(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDatasList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDatasList = rowDatasList;
    }

    public CanalEntry.EventType getEventType() {
        return eventType;
    }

    public void setEventType(CanalEntry.EventType eventType) {
        this.eventType = eventType;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<CanalEntry.RowData> getRowDatasList() {
        return rowDatasList;
    }

    public void setRowDatasList(List<CanalEntry.RowData> rowDatasList) {
        this.rowDatasList = rowDatasList;
    }

    @Override
    public String toString() {
        return "CanalHandle{" +
                "entryType=" + eventType +
                ", tableName='" + tableName + '\'' +
                ", rowDatasList=" + rowDatasList +
                '}';
    }
}
