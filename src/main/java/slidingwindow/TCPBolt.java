package slidingwindow;

import com.google.gson.Gson;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * Created by zuoyuzhu on 2018/3/12.
 */
public class TCPBolt extends BaseBasicBolt {
    public static final Logger LOG = LoggerFactory
            .getLogger(TCPBolt.class);

    private Gson gson;
    private Calendar calendar;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        this.gson = new Gson();
        calendar = Calendar.getInstance();

        super.prepare(stormConf, context);
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // 获取由spout吐过来的数据
        String value = tuple.getStringByField("payment");
        System.out.println("Entering TCPBolt ...");
        System.out.println("TCPBolt Having got TCPBolt value "+value);
        LOG.warn("TCPBolt Having got TCPBolt value "+value);
        if(null == value || "".equals(value)) {
            return ;
        }

        //String paymentInfo = gson.fromJson(value, String.class);

        //calendar.setTime(new Date(paymentInfo));

        // 判断是不是今天下的单（今天是2017.3.31）
        /*if(calendar.get(Calendar.DATE ) == 31) {*/
            basicOutputCollector.emit(new Values(value));
        /*}*/
        System.out.println("Leaving TCPBolt ...");
        LOG.warn("Leaving TCPBolt ...");
    }

    // 设置用于传输的字段
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("payment"));
    }
}
