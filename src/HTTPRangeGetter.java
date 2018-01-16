import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.concurrent.BlockingQueue;

/**
 * A runnable class which downloads a given url.
 * It reads CHUNK_SIZE at a time and writs it into a BlockingQueue.
 * It supports downloading a range of data, and limiting the download rate using a token bucket.
 */
public class HTTPRangeGetter implements Runnable {
    static final int CHUNK_SIZE = 4096;
    static final int MAX_RETRIES = 5;
    private static final int CONNECT_TIMEOUT = 2000;
    private static final int READ_TIMEOUT = 8000; //TODO check this value
    private final String urlString;
    private final Range range;
    private final BlockingQueue<Chunk> outQueue;
    private TokenBucket tokenBucket;
    private Boolean limitDownload;
    private long fileSize;

    HTTPRangeGetter(
            String url,
            Range range,
            BlockingQueue<Chunk> outQueue,
            TokenBucket tokenBucket,
            Boolean limitDownload,
            long fileSize) {
        this.urlString = url;
        this.range = range;
        this.outQueue = outQueue;
        this.tokenBucket = tokenBucket;
        this.limitDownload = limitDownload;
        this.fileSize = fileSize;
    }

    private void downloadRange() throws IOException{
        //TODO need to add sockettimeout exception
        HttpURLConnection httpURLConnection = (HttpURLConnection) new URL(urlString).openConnection();
        httpURLConnection.setConnectTimeout(CONNECT_TIMEOUT);
        httpURLConnection.setReadTimeout(READ_TIMEOUT);
        httpURLConnection.setRequestProperty("Range", "bytes=" + range.getStart() + "-" + range.getEnd());
        int responseCode = httpURLConnection.getResponseCode();
        InputStream inputStream = null;
        for (int i = 1 ; i <= MAX_RETRIES ; i++) {
            try {
                inputStream = httpURLConnection.getInputStream();
                break;
            }catch (SocketTimeoutException e){
                if (i == MAX_RETRIES){
                    //TODO find how to stop all threads
                    System.err.println("got timeout exception. shutting down...");
                    System.exit(1);
                }
            }
        }
        System.out.println("DEBUG: Range: start:" + range.getStart() + " end: " + range.getEnd() +  " Response code: " + responseCode);
//        httpURLConnection.setRequestMethod("GET");
        int lastChunkSize = range.getLength().intValue() % CHUNK_SIZE;
        int i = 0;
        long rangeLength = range.getLength();
        int chunkSize = CHUNK_SIZE;
        long rangesum = range.getStart();
        int part = (int) Math.ceil(rangeLength / (double) CHUNK_SIZE); //calculate how many times we need to iterate to read X chunks in the given range
        //inputStream.skip(range.getStart());
        int getSize = chunkSize;
        while(true) {
            try {
                byte[] byteChunk = new byte[CHUNK_SIZE];
                //jump to the right place in the range to read the next chunk
                //check whether are there enough tokens to read the chunk
                if (limitDownload) {
                    tokenBucket.take(CHUNK_SIZE);
                }
                //case: last chunk is smaller than chunk_size
//            if (lastChunkSize != 0 && i == (part - 1)) {
//                chunkSize = lastChunkSize;
//            }
                if (fileSize == range.getStart() + (CHUNK_SIZE * i) + (rangeLength % CHUNK_SIZE)) {
                    chunkSize = (int) rangeLength % CHUNK_SIZE;
                    byteChunk = new byte[chunkSize];
                }
                //reDo the read operation if the operation reads less than the bytes it should read
                int output = inputStream.read(byteChunk, 0, chunkSize);
                if (range.getEnd() < rangesum) {
                    break;
                } else if (output == -1) {
                    break;
                } else if (output != chunkSize) {
                    int output1 = inputStream.read(byteChunk, output, chunkSize - output);
                    if (output1 != chunkSize - output) {
                        inputStream.read(byteChunk, output, chunkSize - output - output1);
                    }
                }
                outQueue.add(new Chunk(byteChunk, range.getStart() + (CHUNK_SIZE * i), chunkSize));
                i++;
                rangesum += 4096;
            } catch (SocketTimeoutException e){
                System.err.println("got timeout exception. shutting down...");
                System.exit(1);
            }
        }

        System.out.println("finished download");
    }

    @Override
    public void run() {
        try {
            this.downloadRange();
        } catch (IOException e) {
            System.err.println("got IOException. shutting down...");
            System.exit(1);
        }
    }
}
