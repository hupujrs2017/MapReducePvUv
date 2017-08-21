package com.yangyz.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yangyz on 2017/8/21.
 */
public class PvUv {
    static class UbtPageViewMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //AF8950707DA011E6B9B1CC2D8318C2C2 415078 2017-08-12 21:49:39
            String[] line = value.toString().split("\\s+");
            if(line.length<4){
                return;
            }

            String vid = line[0];
            String pageId = line[1];
            String date = line[2];
            String h = line[3];
            if(h.length()<6){
                System.err.println("invalidate time:"+h);
                return;
            }
            Set<String> products = PVGroup.getProductTypeByPageId(pageId);
            if(products.isEmpty()){
//                context.write(new Text(pageId+ "@" + date), new Text(vid + "@" + h.substring(0, 5)));
                return;
            }
            for(String product:products) {
                context.write(new Text(product + "@" + date), new Text(vid + "@" + h.substring(0, 5)));
            }
        }
    }
    static class UbtPageViewReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String pageId = key.toString().split("@")[0]; //1000
            String date = key.toString().split("@")[1];
            Set<String> caches = new HashSet<String>();
            Map<String,List<String>> map = new HashMap<String,List<String>>(); //12:00 -->[vid1,vid2,vid1,vid3]
            ListValueMapWrapper<String,String> setValueMapWrapper = new ListValueMapWrapper<String, String>(map);
            parseVidByMin(values, setValueMapWrapper);
            String min = "00:00";
            int pv=0;
            int uv=0;
            //每天1440分钟
            for(int i=0;i<1440;i++){
                List<String> vids = map.get(min);
                if(vids==null){
                    min=nextMin(min);
                    System.err.println("get vids empty,min:"+min);
                    continue;
                }
                pv = vids.size()+pv;
                uv = getUvByMin(vids, caches)+uv;
                context.write(new Text(pageId),new Text(pv+"\t"+uv+"\t"+date+" "+min+"\t"+date));
                min=nextMin(min);
            }
        }

        private int getUvByMin(List<String> vids,Set<String> caches) {
            int uv = 0;
            for (String vid:vids){
                if(!caches.contains(vid)){
                    uv++;
                    caches.add(vid);
                }
            }
            return uv;
        }

        //输入values[vid1@21:09,vid2@10:08....] listValueMapWrapper<String, String> <12:00，vid1>
        //解析vid、时间
        private void parseVidByMin(Iterable<Text> values, ListValueMapWrapper<String, String> listValueMapWrapper) {
            for (Text v:values){//遍历values获取vid/时间（分钟）
                String[] val = v.toString().split("@");
                if(val.length!=2){
                    System.err.println("err val:"+v.toString());
                    continue;
                }
                String vid = val[0];
                String min = val[1];
                listValueMapWrapper.putForSetValue(min, vid);
            }
        }
        //如果在00:00这一分钟内没有vid，则执行该函数
        public String nextMin(String min){
            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
            Calendar cal = Calendar.getInstance();
            try {
                cal.setTime(sdf.parse(min));
                cal.add(Calendar.MINUTE,1);
                return sdf.format(cal.getTime());
            } catch (ParseException e) {
                return "00:00";
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, ParseException {
        if(args.length!=2){
            System.exit(1);
        }
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJobName("pvByMinJob");
        job.setMapperClass(UbtPageViewMapper.class);
        job.setReducerClass(UbtPageViewReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJarByClass(PvUv.class);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
