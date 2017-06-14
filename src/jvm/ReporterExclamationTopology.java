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
      jedis.flushDB();
      jedis.select(2);
      jedis.flushDB();
      jedis.select(3);
      jedis.flushDB();

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
      Map<String, Integer> ocorrencias;
      int i;
      String item, idReg;

      idReg = reg[0];
      jedis.select(1);
      jedis.set(idReg,sentence);
      jedis.select(2);
      jedis.set(idReg,sentence);


      ocorrencias = new HashMap<String, Integer>();
      for (i = 1;i<reg.length; i++) {
          if (ocorrencias.get(reg[i]) == null) {
              ocorrencias.put(reg[i], 1);
              jedis.select(2);
              jedis.zincrby("rank",1,reg[i]);

              if(reg[i].length() >= 3){
                if (jedis.exists(reg[i])){
                    jedis.select(3);
                    jedis.append(reg[i]," "+idReg);

                }else
                    jedis.set(reg[i],idReg);
                _collector.emit(tuple, new Values(reg[i]+separador+jedis.get(reg[i])));
              }
          }
      }


/*      jedis.select(1);
      jedis.blpop(30,mutex);
      idReg = Integer.toString(Integer.valueOf(jedis.dbSize().toString())+1);
      jedis.lpush(mutex,mutex);
      jedis.set(idReg,idReg+separador+sentence);
      jedis.select(2);
      jedis.set(idReg,idReg+separador+sentence);
  //    jedis.lpush("R"+idReg,"R"+idReg);


      ocorrencias = new HashMap<String, Integer>();
      for (i = 0;i<reg.length; i++) {
          if (ocorrencias.get(reg[i]) == null) {
              ocorrencias.put(reg[i], 1);
              jedis.select(2);
              jedis.zincrby("rank",1,reg[i]);
              if (jedis.exists(reg[i])){
                  jedis.select(3);
                  jedis.append(reg[i]," "+idReg);

              }else
                  jedis.set(reg[i],idReg);
              _collector.emit(tuple, new Values(reg[i]+separador+jedis.get(reg[i])));
          }
      }
*/
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

    builder.setBolt("bolt-indice", new IndiceBolt(), 1).shuffleGrouping("entrada");

    builder.setBolt("bolt-processa", new ProcessaBolt(), 1).shuffleGrouping("bolt-indice");

    builder.setBolt("bolt-tamanho", new SizePositionBolt(), 1).shuffleGrouping("bolt-processa");

    builder.setBolt("bolt-prefixo", new PrefixBolt(), 1).shuffleGrouping("bolt-tamanho");

    builder.setBolt("bolt-sufixo", new SufixBolt(), 1).shuffleGrouping("bolt-prefixo");

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
