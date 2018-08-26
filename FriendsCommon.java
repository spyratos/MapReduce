import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FriendsCommon extends Configured implements Tool {
  
  static Log logMap = LogFactory.getLog(CFMapper.class);
  static Log logReduce = LogFactory.getLog(CFReducer.class);
  public static class CFMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text> {
    
    private Text keyword = new Text();
    private Text friends = new Text();
    public static int count = 0;
    
    public void map(LongWritable key, Text value, 
                    OutputCollector<Text, Text> output, 
                    Reporter reporter) throws IOException {
      String line = value.toString();
      String []meAndFriends = line.split(" ");
      String me = meAndFriends[0];      
      String them = "";
      for(int i=1; i<meAndFriends.length; i++){
        if(i<(meAndFriends.length-1)){
          them += meAndFriends[i]+" ";
        }else{
          them += meAndFriends[i];
        }
      }

      friends.set(them);
      StringTokenizer itr = new StringTokenizer(them);
      while (itr.hasMoreTokens()) {
    	String friend = itr.nextToken().trim();
    	if(me.compareTo(friend) < 0)
    		keyword.set(me + "," +  friend);
    	else
    		keyword.set(friend + "," +  me);
        output.collect(keyword, friends);
      }
    }
  }
  
  public static class CFReducer extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
    public static int count = 0;
    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, 
                       Reporter reporter) throws IOException {
     
      ArrayList<String[]> arr = new ArrayList<String[]>();
      int countTemp = 0;
      while (values.hasNext()) {
    	  String temp = values.next().toString();
    	  String[] listFriend = temp.split(" ");
    	  arr.add(listFriend);
      }
      String commonFriendStr = "";
      HashSet<String> commonElements = new HashSet<String>();
      for(String[] friends: arr){
    	  Arrays.sort(friends);
    	  for(String friend: friends){
    		  if(commonElements.contains(friend)){
    			  commonFriendStr += friend + ",";
    		  }
    		  else
    			  commonElements.add(friend);
    	  }
      }
      Text commonFriendText = new Text();
      if(commonFriendStr.length()>1){
        commonFriendStr=commonFriendStr.substring(0,(commonFriendStr.length()-1));
      }
      commonFriendText.set(commonFriendStr);
      output.collect(key, commonFriendText);
    }
  }
  
  static int printUsage() {
    System.out.println("FriendsCommon [-m <maps>] [-r <reduces>] <input> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }
  
 
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), FriendsCommon.class);
    conf.setJobName("FriendsCommon");
 
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    
    conf.setMapperClass(CFMapper.class); 
    conf.setReducerClass(CFReducer.class);
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    
    List<String> other_args = new ArrayList<String>();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          conf.setNumMapTasks(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {
          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
                           args[i-1]);
        return printUsage();
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: " +
                         other_args.size() + " instead of 2.");
      return printUsage();
    }
    FileInputFormat.setInputPaths(conf, other_args.get(0));
    FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
        
    JobClient.runJob(conf);
    return 0;
  }
  
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FriendsCommon(), args);
    
    System.exit(res);
  }

}

