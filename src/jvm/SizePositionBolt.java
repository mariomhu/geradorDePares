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
  protected int countS1, countS2, countS3;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
      _collector = outputCollector;
      pool = new JedisPool("127.0.0.1");
      countS1=0;
      countS2=0;
      countS3=0;
  }

  @Override
  public void execute(Tuple tuple)
  {
      double parmG     = 0.6;
      Jedis jedis      = pool.getResource();

      String separador = " ";
      Date d = new Date();

      String chave     = tuple.getString(0);
      String registro1 = tuple.getString(1);
      String registro2 = tuple.getString(2);
      String[] reg1    = registro1.split(separador);
      String[] reg2    = registro2.split(separador);
      boolean lOk      = true;
      int i,j,p1=0,p2=9999,tamMax;

      if (reg1.length > reg2.length)
        tamMax = reg1.length;
      else
        tamMax = reg2.length;

      int parmSize = (int) (tamMax*(1-parmG));
      int parmDist = parmSize;

      jedis.select(2);
      String num1 = Integer.toString(countS1);
      jedis.set("OKTAM/"+num1+"/"+registro1+"/"+registro2, "1");
      jedis.set("COUNTS1",num1);
      countS1++;

      matchPair(reg1[0],reg2[0],registro1,registro2,jedis,"R");

      if(reg1.length - reg2.length > parmSize || reg1.length - reg2.length < -parmSize){//Filtro de tamanho
          lOk = false;
      }if(lOk){
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
          matchPair(reg1[0],reg2[0],registro1,registro2,jedis,"OKPOS");
          if(p1-p2 > parmDist || p1-p2 < -parmDist ){
              lOk = false;
          }if(lOk){
               jedis.select(5);
               jedis.set("VPOS/"+registro1+"/"+registro2, "1");
               matchPair(reg1[0],reg2[0],registro1,registro2,jedis,"VVPOS");
               jedis.select(1);
               jedis.set("HORARIO2",Long.toString(d.getTime()));
              _collector.emit(tuple, new Values(registro1,registro2));
          }else{
              matchPair(reg1[0],reg2[0],registro1,registro2,jedis,"VFPOS");
              jedis.select(1);
              jedis.set("HORARIO2",Long.toString(d.getTime()));
          }
      }
      pool.returnResource(jedis);
  }

  public boolean matchPair(String key1,String key2,String string1,String string2,Jedis jedis,String valid){
    String[] s1 = key1.split("-");
    String[] s2 = key2.split("-");
    Boolean par = false;
    jedis.select(5);

    if(s1[1].equals(s2[1])){
      if(s1[2].equals("org") && s2[2].equals("dup")){
        jedis.set(valid+"|"+key1+"|"+key2, string1+"|"+string2);
      }
      if(s1[2].equals("dup") && s2[2].equals("org")){
        jedis.set(valid+"|"+key2+"|"+key1, string2+"|"+string1);
      }
    }
    return par;
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    declarer.declare(new Fields("reg1","reg2"));
    // nothing to add - since it is the final bolt
  }
}
