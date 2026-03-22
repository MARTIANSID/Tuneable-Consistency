package org.example.TokenBucket;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class TokenBucketImpl {

    // jedis for connecting to the master replica of redis
    private Jedis masterJedis;

    // Redis keys — prefixed with server_id to avoid collisions across Raft nodes
    private final String TOKEN_BUCKET_KEY;
    private final String LAST_UPDATE_KEY;
    private final String CURRENT_TERM_KEY;

    // all these thing will be adjusted dynamically later on, right now it is made constant
    private static final double MAX_TOKENS = 9500;
    private static final double REFILL_RATE = 9500; // tokens per second

    public TokenBucketImpl(String redisHost, int redisPort, int serverId) {
        // here first I connect to redis master
        masterJedis = new Jedis(redisHost, redisPort);
        String prefix = "server_" + serverId + "_";
        TOKEN_BUCKET_KEY = prefix + "token_bucket_count";
        LAST_UPDATE_KEY = prefix + "last_update_time";
        CURRENT_TERM_KEY = prefix + "current_term";
    }


    // Get token bucket data from Redis
    // will be in at-least read lock since updateTokens calls this function
    private TokenBucketData getTokenBucketData() {
        String tokenCountStr = masterJedis.get(TOKEN_BUCKET_KEY);
        String lastUpdateTimeStr = masterJedis.get(LAST_UPDATE_KEY);
        String currentTermStr = masterJedis.get(CURRENT_TERM_KEY);
        // if tokenCount is null then we will treat the bucket as full
        double tokenCount = (tokenCountStr == null) ? MAX_TOKENS : Double.parseDouble(tokenCountStr);
        // if lastUpdateTime is null I set it to -1
        long lastUpdateTime = (lastUpdateTimeStr == null) ? -1 : Long.parseLong(lastUpdateTimeStr);
        int currentTerm = 0;
        if(currentTermStr != null) {
            currentTerm = Integer.parseInt(currentTermStr);
        }

        return new TokenBucketData(tokenCount, lastUpdateTime, currentTerm);
    }

    // this should be atleast in read lock
    public TokenBucketData getCurrentTokenBucketData() {
        TokenBucketData data = getTokenBucketData();
        double tokenCount = data.getTokenCount();
        long lastUpdateTime = data.getLastUpdateTime();
        int currentTerm = data.getCurrentTerm();

        // Calculate the elapsed time in nanoseconds
        long currentTime = System.nanoTime();

        // initally when lastUpdateTime = -1 then the newTokenCount should be = MAX_TOKENS
        double newTokens = 0.0, newTokenCount = MAX_TOKENS;

        // if lastUpdateTime = -1 then no need to update anything
        if (lastUpdateTime != -1) {
            long elapsedTime = currentTime - lastUpdateTime;
            newTokens = (elapsedTime * REFILL_RATE) / 1_000_000_000.0; // Convert nanoseconds to seconds
            newTokenCount = tokenCount + newTokens;
        }

        // Ensure token count doesn't exceed the maximum
        if (newTokenCount > MAX_TOKENS) {
            newTokenCount = MAX_TOKENS;
        }
        return new TokenBucketData(newTokenCount, currentTime, currentTerm);
    }

    // Update the redis master with new token count and last update time
    // this function should be inside write lock
    public void updateTokens(double newTokenCount, long lastUpdateTime) {
        Pipeline pipeline = masterJedis.pipelined();

        pipeline.set(TOKEN_BUCKET_KEY, String.valueOf(newTokenCount));
        pipeline.set(LAST_UPDATE_KEY, String.valueOf(lastUpdateTime));

        // Execute the commands asynchronously
        pipeline.sync();
//        System.out.println("Updated token count: " + newTokenCount + " tokens");
    }

    public double getMaxTokens() {
        return MAX_TOKENS;
    }

    public double getRefillRate() {
        return REFILL_RATE;
    }


    public static class TokenBucketData {
        private double tokenCount;
        private long lastUpdateTime;
        private int currentTerm;

        public TokenBucketData(double tokenCount, long lastUpdateTime, int currentTerm) {
            this.tokenCount = tokenCount;
            this.lastUpdateTime = lastUpdateTime;
            this.currentTerm = currentTerm;
        }

        public double getTokenCount() {
            return tokenCount;
        }

        public long getLastUpdateTime() {
            return lastUpdateTime;
        }
        public int getCurrentTerm() {
            return currentTerm;
        }
    }

//     public static void main(String[] args) {
//         TokenBucketImpl tokenBucket = new TokenBucketImpl("127.0.0.1", 6379);

//         // Update the token bucket every second
//         while (true) {
// //            tokenBucket.updateTokens(20);
//             try {
//                 Thread.sleep(1000);  // Sleep for 1 second
//             } catch (InterruptedException e) {
//                 e.printStackTrace();
//             }
//         }
//     }
}
