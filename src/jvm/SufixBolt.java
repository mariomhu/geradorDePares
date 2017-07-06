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
public class SufixBolt extends BaseRichBolt
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
      jedis.set("QTD","0");
  }

  @Override
  public void execute(Tuple tuple)
  {
      double parmG     = 1;
      Jedis jedis      = pool.getResource();
      int offSet;

      double frequencia;
      Date d = new Date();

      String separador = " ";

      String registro1 = tuple.getString(0);
      String registro2 = tuple.getString(1);
      String[] reg1    = registro1.split(separador);
      String[] reg2    = registro2.split(separador);

      int parmNTerms;
      int parmDist;
      double parmFreq  = parmG;//porcentagem

      int minSize = 0,freq = 0;
      int sufSize1, sufSize2, i, j;
      int encontrados = 0;
      boolean lOk = true;
      boolean lFound = false;
      Random gerador = new Random();
      int[] pos;
      int rand = 0;
      int nTerms;
      int posExiste = -1;
      int randomMax;

      offSet = (int) Math.ceil(reg1.length*(1-parmG));
      if (offSet == reg1.length)
          offSet = reg1.length - 1;
      if (offSet < 1)
          offSet = 1;
      minSize = reg1.length - offSet;

      randomMax = reg1.length - offSet;

      parmNTerms = (int) Math.ceil(randomMax*(parmG));
      parmDist = (int) Math.ceil(randomMax*(1-parmG));

          pos = new int[parmNTerms];

          for(i=0;i<pos.length;i++){
              rand = gerador.nextInt(randomMax)+offSet;
              posExiste = -1;
              for(j=0;j<i;j++){
                  if(rand == pos[j]){
                      posExiste = j;
                      break;
                  }
              }
              if(posExiste == -1)
                  pos[i] = rand;
              else
                  i--;
          }

          int contador =0;
          for(i=0;i<pos.length;i++){
              for(j=pos[i]-parmDist;j<=pos[i]+parmDist;j++){
                  if (pos[i] > 0 && pos[i] < reg1.length && j > 0 && j < reg2.length){
                      if(reg1[pos[i]].equals(reg2[j])){
                          lFound = true;
                          encontrados++;
                          break;
                      }
                  }
              }
          }

          jedis.select(7);
          if((double)((double)encontrados/parmNTerms)>=parmFreq){
              jedis.set("V/"+reg1[0]+"/"+reg2[0], "1");

              matchPair(reg1[0],reg2[0],registro1,registro2,jedis,"VV");
              jedis.select(1);
              jedis.set("HORARIO2",Long.toString(d.getTime()));
              _collector.emit(tuple, new Values(registro1,registro2));
          }else{
            jedis.select(1);
            jedis.set("HORARIO2",Long.toString(d.getTime()));
            matchPair(reg1[0],reg2[0],registro1,registro2,jedis,"FV");
          }
    //  }
    //  else
     //     pos = new int[1];
      pool.returnResource(jedis);
  }

  public void matchPair(String key1,String key2,String string1,String string2,Jedis jedis,String valid){
    String[] s1 = key1.split("-");
    String[] s2 = key2.split("-");
    jedis.select(7);

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
