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

    @Override
    public void prepare(
        Map                     map,
        TopologyContext         topologyContext,
        OutputCollector         outputCollector)
    {
        pool = new JedisPool("127.0.0.1");
        String mutex     = "MUTEX"; //token para mutex

    }

    @Override
    public void execute(Tuple tuple)
    {
         String separador = " ";
         String sentence  = tuple.getString(0);
         String[] regs    = sentence.split(separador);
         String chave     = regs[0];
         String[][] info  = new String[regs.length][];
         int[][] infoInt  = new int[regs.length][];
         int i,j;
         Jedis jedis      = pool.getResource();
         String registro;
         int posicao;
         int posicaoAtualizada;
         int aux;
         boolean ordenar = false;
         StringBuilder strBuilder = new StringBuilder();

      //
         jedis.select(2);
        //  if(jedis.zrank("rank",chave)!=null)
              posicao = Integer.valueOf(jedis.zrank("rank",chave).toString());
        //  else
        //      posicao = 1;
            jedis.zincrby("rank",1,chave);
        posicaoAtualizada = Integer.valueOf(jedis.zrank("rank",chave).toString());

         if (posicao != posicaoAtualizada)
             ordenar = true;

         for(i=1;i<regs.length;i++){
             //busca o registro

            // jedis.blpop(30,"R"+regs[i]);
             registro = jedis.get(regs[i]);
             info[i]  = registro.split(separador);
             infoInt[i] = new int[info[i].length];

             //armazena o registro num array de arrays
             for(j=0;j<infoInt[i].length;j++){
//infoInt[30][1000000] = 3;
                 infoInt[i][j] = Integer.valueOf(info[i][j]);
             }
             //verifica se precisa ordenar os registros pré-ordenados
            //  if (ordenar && i < regs.length - 1){
              if (i < regs.length - 1){
                  for(j=1;j<infoInt[i].length-1;j++){
                      if(infoInt[i][j] == Integer.valueOf(chave))
                          if(infoInt[i][j] > infoInt[i][j+1]){
                              aux             = infoInt[i][j];
                              infoInt[i][j]   = infoInt[i][j+1];
                              infoInt[i][j+1] = aux;
                          }
                  }
            //      strBuilder = new StringBuilder();
            //      strBuilder.append(Integer.toString(infoInt[i][0]));
            //      for(j=1;j<infoInt[i].length;i++){
            //          strBuilder.append(" ");
            //          strBuilder.append(Integer.toString(infoInt[i][j]));
            //      }
            //      jedis.set(regs[i],strBuilder.toString());
              }
            //  jedis.lpush("R"+regs[i],"R"+regs[i]);
         }
        //ordena o novo registro inserido
      //   jedis.blpop(30,"R"+regs[regs.length-1]);
         boolean change = false;
         int novo = infoInt.length-1;
         do{
             for(i=1;i<infoInt[novo].length-1;i++){
                 if(infoInt[novo][i] > infoInt[novo][i]+1){
                     aux                = infoInt[novo][i];
                     infoInt[novo][i]   = infoInt[novo][i+1];
                     infoInt[novo][i+1] = aux;
                     change = true;
                }
            }
        }while(change);
        String s = Integer.toString(infoInt[novo][0]);
        for(j=1;j<infoInt[novo].length;i++){
            s += " "+Integer.toString(infoInt[novo][j]);
        }
      /*  strBuilder.delete(0,strBuilder.length());
        strBuilder.append(Integer.toString(infoInt[novo][0]));
        for(j=1;j<infoInt[novo].length;i++){
            strBuilder.append(" ");
            strBuilder.append(Integer.toString(infoInt[novo][j]));
        }*/
        jedis.set(regs[novo],strBuilder.toString());
    //    jedis.lpush("R"+regs[regs.length-1],"R"+regs[regs.length-1]);
        pool.returnResource(jedis);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
      // nothing to add - since it is the final bolt
    }
}
