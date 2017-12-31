import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.BlockingQueue;

/**
 * A runnable class which downloads a given url.
 * It reads CHUNK_SIZE at a time and writs it into a BlockingQueue.
 * It supports downloading a range of data, and limiting the download rate using a token bucket.
 */
public class HTTPRangeGetter implements Runnable {
    static final int CHUNK_SIZE = 4096;
    private static final int CONNECT_TIMEOUT = 500;
    private static final int READ_TIMEOUT = 2000;
    private final String urlString;
    private final Range range;
    private final BlockingQueue<Chunk> outQueue;
    private TokenBucket tokenBucket;
    private Boolean limitDownload;

    HTTPRangeGetter(
            String url,
            Range range,
            BlockingQueue<Chunk> outQueue,
            TokenBucket tokenBucket,
            Boolean limitDownload) {
        this.urlString = url;
        this.range = range;
        this.outQueue = outQueue;
        this.tokenBucket = tokenBucket;
        this.limitDownload = limitDownload;
    }

    private void downloadRange() throws IOException, InterruptedException {
        URL url = new URL (urlString);
        InputStream inputStream = url.openStream();
        long rangeLength = range.getLength();
        int part = (int) Math.ceil(rangeLength / CHUNK_SIZE); //calculate how many times we need to iterate to read X chunks in the given range
        for (int i = 0; i < part; i++) {
            byte[] byteChunk = new byte[4096];
            //jump to the right place in the range to read the next chunk
            inputStream.skip(range.getStart() + (i * CHUNK_SIZE));
            //check whether are there enough tokens to read the chunk
            if(limitDownload) {
                tokenBucket.take(CHUNK_SIZE);
            }
            if (inputStream.read(byteChunk, 0,CHUNK_SIZE) > 0){ //TODO do we need to use offset?
                outQueue.add(new Chunk(byteChunk, 0, CHUNK_SIZE)); //TODO do we need to use offset?
            }
        }
    }

    @Override
    public void run() {
        try {
            this.downloadRange();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            //TODO
        }
    }
}
