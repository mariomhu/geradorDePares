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
  }

  @Override
  public void execute(Tuple tuple)
  {
      double parmG     = 1;
      Jedis jedis      = pool.getResource();
      int prefSize     = 7;
      double size      = 7;
      double parmFreq  = parmG;//0.1;//porcentagem
      double frequencia;
      String separador = " ";
      String registro1 = tuple.getString(0);
      String registro2 = tuple.getString(1);
      String[] reg1    = registro1.split(separador);
      String[] reg2    = registro2.split(separador);
      int i,j,min1,min2,freq = 0;
      int minPre;
      Date d = new Date();

      min1 = (int) Math.ceil(reg1.length*(1-parmG));

      min2 = (int) Math.ceil(reg2.length*(1-parmG));
      if (min1 < 2)
          min1 = 2;
      if (min2 < 2)
          min2 = 2;

      for(i=1;i<min1;i++){
          for(j=1;j<min2;j++){
              if(reg1[i].equals(reg2[j]))
                  freq++;
          }
      }
      if (min1>min2)
        minPre = min2;
      else
        minPre = min1;
      minPre--;
      frequencia = freq;
      jedis.select(1);
      jedis.set("HORARIO2",Long.toString(d.getTime()));
      jedis.select(6);

      if ( reg1[0].equals("rec-188-org")|| reg2[0].equals("rec-188-org") ) {
          jedis.set("VALORES", Double.toString(frequencia)+" "+Integer.toString(minPre));
      }

      if(frequencia/(minPre) >= parmFreq){
        jedis.set("V/"+registro1+"/"+registro2, "1");
          matchPair(reg1[0],reg2[0],registro1,registro2,jedis,"VV");
          _collector.emit(tuple, new Values(registro1,registro2));
      }else{
          matchPair(reg1[0],reg2[0],registro1,registro2,jedis,"FV");
      }
      pool.returnResource(jedis);
  }

  public void matchPair(String key1,String key2,String string1,String string2,Jedis jedis,String valid){
    String[] s1 = key1.split("-");
    String[] s2 = key2.split("-");
    jedis.select(6);

    if(s1[1].equals(s2[1])){
      if(s1[2].equals("org") && s2[2].equals("dup")){
        jedis.set(valid+"|"+key1+"|"+key2, string1+"|"+string2);
      }
      if(s1[2].equals("dup") && s2[2].equals("org")){
        jedis.set(valid+"|"+key2+"|"+key1, string2+"|"+string1);
      }
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    declarer.declare(new Fields("reg1","reg2"));
    // nothing to add - since it is the final bolt
  }

}
