package storm.starter.spout;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;


public class HashTags {
	static String[] hashTags = {"Warcraft","ManOnTheMoon","Diwali","Coldplay","Dawn","halloween","beiber","love","ebola","modi","nyc","girl","suarez","microsoft","india","columbia","mumbai"};
	
	
	static void setMasterListofTags(String[] hashTags_){
		hashTags=hashTags_;
	}
	
	static Set<String> getSubsetOfHashTags(int count){
		Random r = new Random();
		Set<String> subset=new HashSet<String>();
		
		if(count>hashTags.length){
			subset.addAll(Arrays.asList(hashTags));
			return subset;
		}
		
		Set<Integer> processedIndexes=new HashSet<Integer>();
		while(subset.size() < count){
			int index=r.nextInt(hashTags.length);
			if(!processedIndexes.contains(index)){
				subset.add(hashTags[index]);
				processedIndexes.add(index);
			}
		}
		return subset;
	}
	
	static Set<String> getSubsetOfHashTags(){
		return  getSubsetOfHashTags(10);
	}
	
	public static void main(String[] args){
		System.out.println( getSubsetOfHashTags());
	}
}
