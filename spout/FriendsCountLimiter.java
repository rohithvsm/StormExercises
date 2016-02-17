package storm.starter.spout;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;


public class FriendsCountLimiter {
	static int maxLimitOfFriends=5000;
	static Random r = new Random();
	
	public static int getMaxLimitOfFriends() {
		return maxLimitOfFriends;
	}

	public static void setMaxLimitOfFriends(int maxLimitOfFriends) {
		FriendsCountLimiter.maxLimitOfFriends = maxLimitOfFriends;
	}
	
	static int getFriendLimitRandom(int maxLimit){
		return r.nextInt(maxLimit);
	}
	
	static int getFriendLimitRandom(){
		return  getFriendLimitRandom(maxLimitOfFriends);
	}
	public static void main(String[] args){
		System.out.println( getFriendLimitRandom());
	}
}
