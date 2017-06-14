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
public class SizePositionBolt extends BaseRichBolt
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
    }

    @Override
    public void execute(Tuple tuple)
    {
        Jedis jedis      = pool.getResource();
        int parmSize     = 3;
        int parmDist     = 3;
        String separador = " ";

        String chave     = tuple.getString(0);
        String registro1 = tuple.getString(1);
        String registro2 = tuple.getString(2);
        String[] reg1    = registro1.split(separador);
        String[] reg2    = registro2.split(separador);
        boolean lOk      = true;
        int i,j,p1=0,p2=9999;

        jedis.select(8);
        jedis.set("ORI",tuple.getString(0)+"///"+tuple.getString(1)+"///"+tuple.getString(2));
        jedis.set("TAM",Integer.toString(reg1.length)+ " "+Integer.toString(reg2.length)+ " " + Integer.toString(parmSize) );
        if(reg1.length - reg2.length >= parmSize || reg1.length - reg2.length <= -parmSize) //Filtro de tamanho
            lOk = false;
        if(lOk){
            for(i=0;i<reg1.length;i++){
                if(chave.equals(reg1[i])){
                    p1 = i;
                    break;
                }
            }
            for(i=0;i<reg2.length;i++){
                if(chave.equals(reg2[i])){
                    p2 = i;
                    break;
                }
            }
            if(p1-p2 > parmDist || p1-p2 < -parmDist )
                lOk = false;
            if(lOk){
                 jedis.select(8);
                 jedis.set("PAR",registro1+"/"+registro2);
                _collector.emit(tuple, new Values(registro1,registro2));
            }else{
                jedis.select(8);
                jedis.set("FAL",registro1+"/"+registro2+"/"+Integer.toString(p1)+"/"+Integer.toString(p2));
            }
        }
        pool.returnResource(jedis);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
      declarer.declare(new Fields("reg1","reg2"));
      // nothing to add - since it is the final bolt
    }
}
