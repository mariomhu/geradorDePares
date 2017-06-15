package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

import java.util.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;


/**
 * A bolt that prints the word and count to redis
 */
public class PrefixBolt extends BaseRichBolt
{
    OutputCollector _collector;
    protected JedisPool pool;

    @Override
    public void prepare(
        Map                     map,
        TopologyContext         topologyContext,
        OutputCollector         outputCollector)
    {
        _collector = outputCollector;
        pool = new JedisPool("127.0.0.1");
        Jedis jedis      = pool.getResource();
        jedis.select(8);
        jedis.set("QTD1","0");
    }

    @Override
    public void execute(Tuple tuple)
    {
        Jedis jedis      = pool.getResource();
        int prefSize     = 7;
        double size      = 7;
        double parmFreq  = 0.1;//porcentagem
        double frequencia;
        String separador = " ";
        String registro1 = tuple.getString(0);
        String registro2 = tuple.getString(1);
        String[] reg1    = registro1.split(separador);
        String[] reg2    = registro2.split(separador);
        int i,j,min1,min2,freq = 0;

        if(prefSize + 1 < reg1.length)
            min1 = prefSize + 1;
        else
            min1 = reg1.length;

        if(prefSize + 1 < reg2.length)
            min2 = prefSize + 1;
        else
            min2 = reg2.length;

        for(i=1;i<min1;i++){
            for(j=1;j<min2;j++){
                if(reg1[i].equals(reg2[j]))
                    freq++;
            }
        }
        jedis.select(8);
        jedis.set("MIN1",Integer.toString(min1));
        jedis.set("MIN2",Integer.toString(min2));
        frequencia = freq;

        if(frequencia > 0)
          jedis.set("FREQ",Double.toString(frequencia));
        if(frequencia/size > parmFreq){
            int qtd;
            String strAux;
            strAux = jedis.get("QTD1");
            qtd = Integer.valueOf(strAux);
            jedis.set("QTD1",Integer.toString(qtd+1));
            jedis.set("PAR1",registro1+"/"+registro2);
            _collector.emit(tuple, new Values(registro1,registro2));
        }else{
            jedis.set("FAL1",registro1+"/"+registro2);
        }
        pool.returnResource(jedis);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
      declarer.declare(new Fields("reg1","reg2"));
      // nothing to add - since it is the final bolt
    }
}
