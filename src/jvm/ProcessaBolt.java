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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.*;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

/**
 * A bolt that prints the word and count to redis
 */
public class ProcessaBolt extends BaseRichBolt
{
  OutputCollector _collector;
  protected JedisPool pool;
  protected int count,co;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
      _collector = outputCollector;
      pool = new JedisPool("127.0.0.1");
      count = 0;
      co = 0;
  }

  @Override
  public void execute(Tuple tuple)
  {
       Date d = new Date();
       String separador = " ";
       String sentence  = tuple.getString(0);
       String[] regs    = sentence.split(separador);
       String chave     = regs[0];
       int i,j;
       Jedis jedis      = pool.getResource();
       String registro;
       int aux;
       int n = regs.length -1;
       boolean change = false;
       Map<String, Integer> ocorrencias;
       String[] info;
       int[] infoInt;
       String[] regComp;
       int[] regCompInt;
       int k;
       String registros, idReg;
       String auxString;
       String saida, saida1, saida2,auxS;

       ocorrencias = new HashMap<String, Integer>();

       jedis.select(6);
       String num1 = Integer.toString(count);
       jedis.set(num1+"/"+sentence, "1");
       jedis.set("COUNTP",num1);
       count++;


       jedis.select(1);
       int cont = 1;
       registro = jedis.get(regs[n]);
       info     = registro.split(separador);
       infoInt  = new int[info.length];
      for(i=1;i<info.length;i++){
           if (ocorrencias.get(info[i]) == null) {
               jedis.select(2);
               auxS = "1";
               if (jedis.zrank("rank",info[i]) != null)
                   auxS = jedis.zrank("rank",info[i]).toString();
               infoInt[i] = Integer.valueOf(auxS);
               ocorrencias.put(info[i], infoInt[i]);

           }else
               infoInt[i] = ocorrencias.get(info[i]);

              jedis.set("CONT","T"+Integer.valueOf(cont));
              cont++;
       }
       String registroInt = " | ";
      do{
           change = false;

           for(i=1;i<infoInt.length-1;i++){

               if(infoInt[i] > infoInt[i+1]){
                   aux          = infoInt[i];
                   infoInt[i]   = infoInt[i+1];
                   infoInt[i+1] = aux;

                   auxString = info[i];
                   info[i]   = info[i+1];
                   info[i+1] = auxString;

                   change = true;
              }
          }
      }while(change);
  //    jedis.set(idReg,registroInt);
      saida1 = "";
      for(k=0;k<info.length;k++){
          saida1 += info[k];
          if(k<info.length-1)
             saida1 += " ";
      }
      jedis.select(1);
      jedis.set(info[0],saida1);


      if (regs.length >= 3){
          co++;

       for(i=1;i<regs.length -1;i++){
            jedis.select(1);
            registros   = jedis.get(regs[i]);
            regComp     = registros.split(separador);
            regCompInt  = new int[regComp.length];


           for(j=1;j<regComp.length;j++){
                if (ocorrencias.get(regComp[j]) == null) {
                    jedis.select(2);
                    auxS = jedis.zrank("rank",regComp[j]).toString();
                    regCompInt[j] = Integer.valueOf(auxS);
                    ocorrencias.put(regComp[j], regCompInt[j]);
                }else
                    regCompInt[j] = ocorrencias.get(regComp[j]);
            }

          do{
                change = false;
                for(j=1;j<regCompInt.length-1;j++){
                    if(regCompInt[j] > regCompInt[j+1]){
                       aux             = regCompInt[j];
                       regCompInt[j]   = regCompInt[j+1];
                       regCompInt[j+1] = aux;

                       auxString    = regComp[j];
                       regComp[j]   = regComp[j+1];
                       regComp[j+1] = auxString;

                       change = true;
                  }
              }
           }while(change);

           saida2 = "";

           for(k=0;k<regComp.length;k++){
               saida2 += regComp[k];
               if(k<regComp.length-1)
                  saida2 += " ";
           }

           matchPair(info[0],regComp[0],saida1,saida2,jedis);

            jedis.select(1);
            jedis.set("HORARIO2",Long.toString(d.getTime()));
            _collector.emit(tuple, new Values(chave,saida1,saida2));
         }
        }

      pool.returnResource(jedis);
  }

  public void matchPair(String key1,String key2,String string1,String string2,Jedis jedis){
    String[] s1 = key1.split("-");
    String[] s2 = key2.split("-");
    jedis.select(4);

    if(s1[1].equals(s2[1])){
      if(s1[2].equals("org") && s2[2].equals("dup")){
        jedis.set(key1+"|"+key2, string1+"|"+string2);
      }
      if(s1[2].equals("dup") && s2[2].equals("org")){
        jedis.set(key2+"|"+key1, string2+"|"+string1);
      }
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    declarer.declare(new Fields("chave","saida1","saida2"));
    // declarer.declare(new Fields("exclamated-word"));
    // nothing to add - since it is the final bolt
  }
}
