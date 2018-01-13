/**
 * A token bucket based rate-limiter.
 *
 * This class should implement a "soft" rate limiter by adding maxBytesPerSecond sumTokens to the bucket every second,
 * or a "hard" rate limiter by resetting the bucket to maxBytesPerSecond sumTokens every second.
 */
public class RateLimiter implements Runnable {
    private final TokenBucket tokenBucket;
    private final Long maxBytesPerSecond;

    RateLimiter(TokenBucket tokenBucket, Long maxBytesPerSecond) {
        this.tokenBucket = tokenBucket;
        this.maxBytesPerSecond = maxBytesPerSecond;
    }

    @Override
    public void run() {
        while (true) {
            tokenBucket.set(maxBytesPerSecond);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.err.println("sleep function has failed");
                    e.printStackTrace();
                }
            if (tokenBucket.terminated()) { //TODO is it the best way?
                break;
            }
        }
    }
}
