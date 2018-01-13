import java.util.concurrent.atomic.AtomicLong;

/**
 * A Token Bucket (https://en.wikipedia.org/wiki/Token_bucket)
 *
 * This thread-safe bucket should support the following methods:
 *
 * - take(n): remove n sumTokens from the bucket (blocks until n sumTokens are available and taken)
 * - set(n): set the bucket to contain n sumTokens (to allow "hard" rate limiting)
 * - add(n): add n sumTokens to the bucket (to allow "soft" rate limiting)
 * - terminate(): mark the bucket as terminated (used to communicate between threads)
 * - terminated(): return true if the bucket is terminated, false otherwise
 *
 */

class TokenBucket {
    private AtomicLong tokens;
    private boolean terminated;

    TokenBucket(long tokens) {
        terminated = false;
        this.tokens = new AtomicLong();
        this.tokens.set(tokens);
    }

    void take(long tokens) {
        while (this.tokens.get() < tokens){ //not enough available tokens in the bucket
        }
        this.tokens.getAndAdd(tokens * -1);
    }

    void terminate() {
        terminated = true;
    }

    boolean terminated() {
        return terminated;
    }

    void set(long tokens) {
        this.tokens.set(tokens);
    }
}
