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
        _collector = outputCollector;
        pool = new JedisPool("127.0.0.1");
    }

    @Override
    public void execute(Tuple tuple)
    {
         String separador = " ";
         String sentence  = tuple.getString(0);
         String[] regs    = sentence.split(separador);
         String chave     = regs[0];
         //String[][] info  = new String[regs.length][];

        // int[][] infoInt  = new int[regs.length][];
         int i,j;
         Jedis jedis      = pool.getResource();
         String registro;
         int posicao;
         int posicaoAtualizada;
         int aux;
         int n = regs.length -1;
         boolean ordenar = false;
         boolean change = false;
         StringBuilder strBuilder = new StringBuilder();
         Map<String, Integer> ocorrencias;
         String[] info;
         int[] infoInt;
         String[] regComp;
         int[] regCompInt;
         int frequencia,agoraVai;
         int k;
         String registros, idReg;
         String auxString;
         String saida, saida1, saida2,auxS;

         ocorrencias = new HashMap<String, Integer>();

         jedis.select(1);
         int cont = 1;
         int cont2 = 1;
         registro = jedis.get(regs[n]);
         info     = registro.split(separador);
         infoInt  = new int[info.length];
         cont2++;
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
             jedis.select(10);

             idReg = Integer.toString(Integer.valueOf(jedis.dbSize().toString())+1);

             for(i=1;i<infoInt.length-1;i++){

                 if(infoInt[i] > infoInt[i+1]){
                    // jedis.set("MAIOR","MAIOR");
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


        if (regs.length > 3){
         for(i=2;i<regs.length -1;i++){
              jedis.select(1);
              registros   = jedis.get(regs[i]);
              regComp     = registros.split(separador);
              regCompInt  = new int[regComp.length];


             for(j=1;j<regComp.length;j++){
                  if (ocorrencias.get(regComp[j]) == null) {
                      jedis.select(7);
                      jedis.set("chave",regComp[j]);
                      jedis.select(2);
                      auxS = jedis.zrank("rank",regComp[j]).toString();
                      //frequencia = Integer.valueOf();
                      regCompInt[j] = Integer.valueOf(auxS);
                      //regCompInt[i] = frequencia;
                      ocorrencias.put(regComp[j], regCompInt[j]);
                  }else
                      regCompInt[j] = ocorrencias.get(regComp[j]);
              }

            do{
                  change = false;
                  for(j=1;j<regCompInt.length-1;j++){
                      if(regCompInt[j] > regCompInt[j+1]/* || (regCompInt[j] == regCompInt[j+1] && regComp[j] == info[j])*/){
                          jedis.select(10);
                          jedis.set("MAIOR","MAIOR");
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
          //

             for(k=0;k<regComp.length;k++){
                 saida2 += regComp[k];
                 if(k<regComp.length-1)
                    saida2 += " ";
             }

             jedis.select(1);
             jedis.set(regComp[0],saida2);
             saida = saida1 + "|" + saida2;
              jedis.select(9);
          //    idReg = Integer.toString(Integer.valueOf(jedis.dbSize().toString())+1);
              jedis.set("CHAVE",chave);
              jedis.set("SAIDA1",saida1);
              jedis.set("SAIDA2",saida2);
              //jedis.set("SAIDA",chave+"|"+saida);
              //String emissao = chave+"|"+saida;
              // _collector.emit(tuple, new Values(chave+"|"+saida));
              _collector.emit(tuple, new Values(chave,saida1,saida2));
           }
          }
        //     */
        /*     saida1 = "";
             jedis.select(5);
             idReg = Integer.toString(Integer.valueOf(jedis.dbSize().toString())+1);
             for(i=0;i<info.length;i++)
                 saida1 += info[i] + " ";
             jedis.set(idReg,saida1); */
             //_collector.emit(tuple, new Values(reg[i]+separador+jedis.get(reg[i])));
             //busca o registro

            // jedis.blpop(30,"R"+regs[i]);
    /*         registro = jedis.get(regs[i]);
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
            s += " "+Integer.toString(infoInt[novo][j]);*/
      //  }
      /*  strBuilder.delete(0,strBuilder.length());
        strBuilder.append(Integer.toString(infoInt[novo][0]));
        for(j=1;j<infoInt[novo].length;i++){
            strBuilder.append(" ");
            strBuilder.append(Integer.toString(infoInt[novo][j]));
        }*/
    //    jedis.set(regs[novo],strBuilder.toString());
    //    jedis.lpush("R"+regs[regs.length-1],"R"+regs[regs.length-1]);
        pool.returnResource(jedis);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
      declarer.declare(new Fields("chave","saida1","saida2"));
      // declarer.declare(new Fields("exclamated-word"));
      // nothing to add - since it is the final bolt
    }
}
