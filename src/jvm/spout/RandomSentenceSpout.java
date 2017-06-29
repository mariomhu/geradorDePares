package udacity.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class RandomSentenceSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  //Random _rand;
  private BufferedReader reader;

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    //_rand = new Random();

    try {
      reader = new BufferedReader(new FileReader("/home/db-user/repositório/geradorDePares/src/jvm/spout/teste_mario2"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void nextTuple() {
/*    Utils.sleep(100);
    String[] sentences = new String[]{
      "the cow jumped over the moon",
      "an apple a day keeps the doctor away",
      "four score and seven years ago",
      "snow white and the seven dwarfs",
      "i am at two with nature"
      };
    String sentence = sentences[_rand.nextInt(sentences.length)];
    _collector.emit(new Values(sentence));*/

    try {
            String line = reader.readLine();
            if (line != null) {
                this._collector.emit(new Values(line));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sentence"));
  }

}
