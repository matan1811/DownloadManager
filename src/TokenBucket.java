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
    long sumTokens;
    boolean terminated;
    final long maxTokens = 10000;
    TokenBucket() {
        terminated = false;
        sumTokens = maxTokens;
    }

    synchronized void take(long tokens) {
        while (sumTokens < tokens){ //not enough available tokens in the bucket
        }
        sumTokens -= tokens;
    }

    void terminate() {
        terminated = true;
    }

    boolean terminated() {
        return terminated;
    }

    void set(long tokens) {
        sumTokens = tokens;
    }

    void add(long tokens) {
        sumTokens += tokens;
        if (sumTokens > maxTokens) {
            sumTokens = maxTokens;
        }
    }
}
