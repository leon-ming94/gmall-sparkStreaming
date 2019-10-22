import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author shkstart
 * @create 2019-10-22 11:52
 */
public class CanalApp {

    public static void main(String[] args) {
        // TODO 连接canal服务
        CanalConnector connect = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop112", 11111),
                "example",
                "", "");

        while (true) {
            // TODO 从canal服务拉取数据
            connect.connect();
            connect.subscribe("");
            Message message = connect.get(100);
            // 假如没有抓取到数据 等5秒再拉取
            if (message.getEntries().size() == 0) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                // TODO 拉取数据后 解析数据
                List<CanalEntry.Entry> entries = message.getEntries();
                for (CanalEntry.Entry entry : entries) {
                    if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            CanalHandle canalHandle = new CanalHandle(eventType, entry.getHeader().getTableName(), rowDatasList);
                            canalHandle.handle();
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }


            }

        }


        // TODO 处理数据    往kafka发送数据到相应的topic中

    }

}
