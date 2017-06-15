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
        Jedis jedis      = pool.getResource();
        int offSet       = 7;
        int posMin       = offSet + 1;
        int parmSearchDist = 2;
        int parmNTerms   = 4;
        int parmDist     = 4;
        double size      = 3;
        double parmFreq  = 0.1;//porcentagem
        double frequencia;

        String separador = " ";


        String registro1 = tuple.getString(0);
        String registro2 = tuple.getString(1);
        String[] reg1    = registro1.split(separador);
        String[] reg2    = registro2.split(separador);
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
        int randomMax = reg1.length - offSet;

        sufSize1 = reg1.length - posMin;
        sufSize2 = reg2.length - posMin;

        if(sufSize1 <= 0 || sufSize2 <= 0)
            lOk = false;
        if(lOk){
        //    if(sufSize1 <= sufSize2)
                minSize = sufSize1;
        //    else
        //        minSize = sufSize2;

            if(minSize < parmNTerms)
                pos = new int[minSize];
            else
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

            jedis.select(8);
            for(i=0;i<pos.length;i++){
                for(j=pos[i]-parmDist;j<=pos[i]+parmDist;j++)
                    jedis.set("ERROR",registro1+"|"+registro2+"|i"+Integer.toString(i)+" j"+Integer.toString(j)+" r"+Integer.toString(randomMax)+" v"+Integer.toString(pos[i])+" m"+Integer.toString(minSize)+" pm"+Integer.toString(posMin));
                     if (pos[i] > 0 && pos[i] < reg1.length && j > 0 && j < reg2.length)
                         if(reg1[pos[i]].equals(reg2[j])){
                             lFound = true;
                             encontrados++;
                             break;
                         }
            }

            String positions = "";
            for(i=0;i<pos.length;i++){
                positions += Integer.toString(pos[i]) + " ";
            }
            if((double)((double)encontrados/parmNTerms)>=parmFreq){
                int qtd;
                String strAux;
                strAux = jedis.get("QTD");
                qtd = Integer.valueOf(strAux);
                jedis.set("QTD",Integer.toString(qtd+1));
                jedis.set("PAR2"+strAux,registro1+"/"+registro2+"/"+positions);
                _collector.emit(tuple, new Values(registro1,registro2));
            }else{
                jedis.set("FAL2",registro1+"/"+registro2+"/"+positions);
            }
        }
        else
            pos = new int[1];
        pool.returnResource(jedis);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("reg1","reg2"));
        // nothing to add - since it is the final bolt
    }
}
