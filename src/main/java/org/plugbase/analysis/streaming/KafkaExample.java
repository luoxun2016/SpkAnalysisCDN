package org.plugbase.analysis.streaming;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import org.plugbase.analysis.config.Configuration;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaExample {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("metadata.broker.list", "spark0:9092,spark6:9092,spark7:9092,spark8:9092,spark9:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String,String>(config);
		
		String path = "J:\\log\\accesslog";
		File file = new File(path);
		for(File f : file.listFiles()){
			if(!f.isDirectory()) continue;
			for(File fs : f.listFiles()){
				long start = System.currentTimeMillis();
				if(fs.getName().endsWith(".zip")) continue;
				BufferedReader reader = new BufferedReader(new FileReader(fs));
				String s = null;
				while((s = reader.readLine())!= null){
					producer.send(new KeyedMessage<String, String>(Configuration.TOPIC_CDNLOG, s));
				}
				reader.close();
				
				System.out.println(fs.getName() + "===>"+(System.currentTimeMillis()-start)+"ms");
			}
		}
	}
}
