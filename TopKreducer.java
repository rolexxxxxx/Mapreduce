package main.java.mapreduce;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKreducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	////// Define a minimum heap class (size = K) to store WORD:COUNT prioritized by count
	public static class HeapElement {
		String word;
		int val;
		HeapElement(String word, int val) {
			this.word = word;
			this.val = val;
		}
	}
	////// END define
	
	
	///// NEWLY ADDED
	private int K;
	private PriorityQueue<HeapElement> pq;
	@Override
	public void setup (Context context) throws IOException, InterruptedException {
		K = context.getConfiguration().getInt("K", 10);
		pq = new PriorityQueue<HeapElement>(K, new Comparator<HeapElement>() {  
			public int compare(HeapElement a, HeapElement b) {
	    		return a.val - b.val;
			}      
	    });
	}
	///// END NEWLY ADDED
	
	
	//private IntWritable result = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		/*
		result.set(sum);
		context.write(key, result);
		*/
		
		////// REWRITE
		if(pq.size() < K) {
			pq.offer(new HeapElement(key.toString(), sum));
		} else if (pq.peek().val < sum) {
			pq.poll();
			pq.offer(new HeapElement(key.toString(), sum));
		}
		////// END REWRITE
	}
	
	////// Write to the results
	protected void cleanup(Context context) throws IOException, InterruptedException {
        HeapElement[] elem = new HeapElement[pq.size()];
        int i = pq.size();
        while(!pq.isEmpty()){
            elem[--i] = pq.poll();
        }
        for(HeapElement it : elem){
            context.write(new Text(it.word), new IntWritable(it.val));
        }
    }
}
