package slidingwindow;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by zuoyuzhu on 2018/3/13.
 */
public class MessageScheme implements Scheme {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageScheme.class);

    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer) {
        try {
            return new Values(deserializeString(byteBuffer));
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            LOGGER.error("Cannot parse the provided message");
        }
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("msg");
    }

    public static String deserializeString(ByteBuffer string) throws UnsupportedEncodingException {
        if (string.hasArray()) {
            int base = string.arrayOffset();
            return new String(string.array(), base + string.position(), string.remaining());
        } else {
            return new String(Utils.toByteArray(string), "UTF-8");
        }
    }

}
