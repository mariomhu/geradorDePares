package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.Map;

// import org.apache.storm.redis.bolt.AbstractRedisBolt;
// import org.apache.storm.redis.bolt.RedisStoreBolt;
// import org.apache.storm.redis.common.config.JedisClusterConfig;
// import org.apache.storm.redis.common.config.JedisPoolConfig;
// import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
// import org.apache.storm.redis.common.mapper.RedisLookupMapper;
// import org.apache.storm.redis.common.mapper.RedisStoreMapper;
// import org.apache.storm.redis.trident.state.RedisState;
// import org.apache.storm.redis.trident.state.RedisStateQuerier;
// import org.apache.storm.redis.trident.state.RedisStateUpdater;
// import org.apache.storm.shade.com.google.common.collect.Lists;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.Map;
import java.util.*;

import udacity.storm.spout.RandomSentenceSpout;

public class ReporterExclamationTopology {

  public static class IndiceBolt extends BaseRichBolt
  {
    OutputCollector _collector;
    protected JedisPool pool;

    @Override
    public void prepare(
        Map                     map,
        TopologyContext         topologyContext,
        OutputCollector         collector)
    {
      _collector = collector;
      pool = new JedisPool("127.0.0.1");
      String mutex         = "MUTEX"; //token para mutex
      Jedis jedis      = pool.getResource();
      int db;

      jedis.select(1);
      jedis.lpush(mutex,mutex);
      jedis.select(2);
      jedis.lpush(mutex,mutex);
      jedis.select(3);
      jedis.lpush(mutex,mutex);
      jedis.select(4);
      jedis.lpush(mutex,mutex);
      pool.returnResource(jedis);

    }
    // 1 - registros originais  2 - registros mapeados/ordenados  3 - mapeamento  4- índice invertido
    @Override
    public void execute(Tuple tuple)
    {

      String separador = " ";
      String mutex     = "MUTEX";
      String rank      = "rank";
      String sentence  = tuple.getString(0);
      Jedis jedis      = pool.getResource();
      String[] reg     = sentence.split(separador);
      int[] regInt     = new int[reg.length];
      String[] regMap  = new String[reg.length];
      Map<String, Integer> ocorrencias;
      int i;
      String item, idReg;

      jedis.select(1);
      jedis.blpop(30,mutex);
      idReg = Integer.toString(Integer.valueOf(jedis.dbSize().toString())+1);
      jedis.lpush(mutex,mutex);
      jedis.set(idReg,idReg+separador+sentence);
      jedis.select(3);
      for(i = 0;i<reg.length;i++){
          if (!jedis.exists(reg[i])){
              jedis.blpop(30,mutex);
              regMap[i] = Integer.toString(Integer.parseInt(jedis.dbSize().toString())+1);
              jedis.lpush(mutex,mutex);
              jedis.set(reg[i],regMap[i]);
          }
          else
              regMap[i] = jedis.get(reg[i]).toString();
      }

      StringBuilder strBuilder = new StringBuilder();
      strBuilder.append(idReg);
      strBuilder.append(" ");
      for (i = 0;i<regMap.length; i++) {
          strBuilder.append(regMap[i]);
          if (i<reg.length - 1)
              strBuilder.append(" ");
      }
      String regMapeado = strBuilder.toString();
      jedis.select(2);
      jedis.lpush("R"+idReg,"R"+idReg);
      jedis.set(idReg,regMapeado);

      jedis.select(4);
      ocorrencias = new HashMap<String, Integer>();
      for (i = 0;i<regMap.length; i++) {
          if (ocorrencias.get(regMap[i]) == null) {
              ocorrencias.put(regMap[i], 1);
              if (jedis.exists(regMap[i]))
                  jedis.append(regMap[i]," "+idReg);
              else
                  jedis.set(regMap[i],idReg);
              _collector.emit(tuple, new Values(regMap[i]+separador+jedis.get(regMap[i]).toString()));
          }
      }

      pool.returnResource(jedis);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
      declarer.declare(new Fields("exclamated-word"));
    }
  }

  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("entrada", new RandomSentenceSpout(), 1);

    builder.setBolt("bolt-indice", new IndiceBolt(), 3).shuffleGrouping("entrada");

    builder.setBolt("bolt-processa", new ProcessaBolt(), 3).shuffleGrouping("bolt-indice");

    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // run it in a simulated local cluster

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("exclamation", conf, builder.createTopology());

      // let the topology run for 30 seconds. note topologies never terminate!
      Thread.sleep(300000);

      // kill the topology
      cluster.killTopology("exclamation");

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}
