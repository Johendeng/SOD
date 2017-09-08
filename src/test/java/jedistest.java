import com.yk.sod.entity.Record;
import com.yk.sod.util.SerializeUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

/**
 * Created by dengtianjia on 2017/9/8.
 */
public class jedistest {

    public static void main(String[] args){
        Jedis jedis = new Jedis("10.0.104.19",6379);
        byte[] rec  = jedis.get("totalRecord".getBytes());
//        Record record = (Record) SerializeUtil.unserialize(rec);
        String arr = jedis.get("abc");
        System.out.println(arr);
//        System.out.println(record.getNameRecord().toString());
//        System.out.println(record.getTypeRecord().toString());
//        System.out.println(record.getPriceRecord().toString());

    }
}
