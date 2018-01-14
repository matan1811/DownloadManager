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

    private void downloadRange() throws IOException, InterruptedException {
        //TODO need to add sockettimeout exception
        System.out.println("DEBUG: Range: start:" + range.getStart() + " end: " + range.getEnd());
        HttpURLConnection httpURLConnection = (HttpURLConnection) new URL(urlString).openConnection();
        httpURLConnection.setConnectTimeout(CONNECT_TIMEOUT);
        httpURLConnection.setReadTimeout(READ_TIMEOUT);
        httpURLConnection.setRequestProperty("Range", "bytes=" + range.getStart() + "-" + range.getEnd());
//        httpURLConnection.setRequestMethod("GET");
        InputStream inputStream = httpURLConnection.getInputStream();
        int lastChunkSize = range.getLength().intValue() % CHUNK_SIZE;
        int i = 0;
        long rangeLength = range.getLength();
        int chunkSize = CHUNK_SIZE;
        long rangesum = range.getStart();
        int part = (int) Math.ceil(rangeLength / (double) CHUNK_SIZE); //calculate how many times we need to iterate to read X chunks in the given range
        inputStream.skip(range.getStart());
        int getSize = chunkSize;
        while(true) {
            byte[] byteChunk = new byte[CHUNK_SIZE];
            //jump to the right place in the range to read the next chunk
            //check whether are there enough tokens to read the chunk
            if(limitDownload) {
                tokenBucket.take(CHUNK_SIZE);
            }
            //case: last chunk is smaller than chunk_size
//            if (lastChunkSize != 0 && i == (part - 1)) {
//                chunkSize = lastChunkSize;
//            }
            if (fileSize  == range.getStart() + (CHUNK_SIZE * i) + (rangeLength % CHUNK_SIZE)){
                chunkSize = (int) rangeLength % CHUNK_SIZE;
                byteChunk = new byte[chunkSize];
            }
            //reDo the read operation if the operation reads less than the bytes it should read
            int output = inputStream.read(byteChunk,0,chunkSize);
            if (range.getEnd() < rangesum) {
                break;
            } else if (output == -1){
                break;
            } else if (output != chunkSize) {
                int output1 = inputStream.read(byteChunk,output,chunkSize - output);
                if (output1 != chunkSize - output){
                    inputStream.read(byteChunk,output,chunkSize - output -output1);
                }
            }
            outQueue.add(new Chunk(byteChunk, range.getStart() + (CHUNK_SIZE * i), chunkSize));
            i++;
            rangesum += 4096;
        }

        System.out.println("finished download");
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
