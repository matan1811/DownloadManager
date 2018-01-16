import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.*;

public class IdcDm {

    /**
     * Receive arguments from the command-line, provide some feedback and start the download.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        int numberOfWorkers = 1;
        Long maxBytesPerSecond = null;

        if (args.length < 1 || args.length > 3) {
            System.err.printf("usage:\n\tjava IdcDm URL [MAX-CONCURRENT-CONNECTIONS] [MAX-DOWNLOAD-LIMIT]\n");
            System.exit(1);
        } else if (args.length >= 2) {
            numberOfWorkers = Integer.parseInt(args[1]);
            if (args.length == 3)
                maxBytesPerSecond = Long.parseLong(args[2]);
        }

        String url = args[0];

        System.err.printf("Downloading");
        if (numberOfWorkers > 1)
            System.err.printf(" using %d connections", numberOfWorkers);
        if (maxBytesPerSecond != null)
            System.err.printf(" limited to %d Bps", maxBytesPerSecond);
        System.err.printf("...\n");

        DownloadURL(url, numberOfWorkers, maxBytesPerSecond);
    }

    /**
     * Initiate the file's metadata, and iterate over missing ranges. For each:
     * 1. Setup the Queue, TokenBucket, DownloadableMetadata, FileWriter, RateLimiter, and a pool of HTTPRangeGetters
     * 2. Join the HTTPRangeGetters, send finish marker to the Queue and terminate the TokenBucket
     * 3. Join the FileWriter and RateLimiter
     *
     * Finally, print "Download succeeded/failed" and delete the metadata as needed.
     *
     * @param url URL to download
     * @param numberOfWorkers number of concurrent connections
     * @param maxBytesPerSecond limit on download bytes-per-second
     */
    private static void DownloadURL(String url, int numberOfWorkers, Long maxBytesPerSecond) {
        FileOutputStream tempMetadata = null;
        long fileSize = getFileSize(url);
//        try {
//            tempMetadata = new FileOutputStream("C:\\Users\\matan\\Google Drive\\CS2015_6\\Year3\\net\\DownloaManager\\test");
//            ObjectOutputStream tmpObjectOutputStream = new ObjectOutputStream(tempMetadata);
//            tmpObjectOutputStream.writeObject(new DownloadableMetadata(url, fileSize));
//        } catch (IOException | ClassNotFoundException e) {
//            e.printStackTrace();
//        }
        boolean limitDownload = false;
        TokenBucket tokenBucket = null;
        Thread rateLimiter = null;
        if (maxBytesPerSecond != null){
            limitDownload = true;
            tokenBucket = new TokenBucket(maxBytesPerSecond);
            rateLimiter = new Thread(new RateLimiter(tokenBucket,maxBytesPerSecond));
            rateLimiter.start();
        }
        long chunksize = HTTPRangeGetter.CHUNK_SIZE;
        System.out.println("DEBUG: FileSize: " + fileSize);
        long leftRange = (numberOfWorkers % numberOfWorkers) * chunksize;
        int numOfChunks = (int) Math.ceil(fileSize / (double) chunksize);
        BlockingQueue<Chunk> chunkQueue = new ArrayBlockingQueue<Chunk>(numOfChunks);
        Thread fileWriter = null;
        DownloadableMetadata downloadableMetadata = null;
        try {
            downloadableMetadata = new DownloadableMetadata(url, fileSize);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        fileWriter = new Thread(new FileWriter(downloadableMetadata, chunkQueue));
        fileWriter.start();
        //LOOP
        Range missingRange = downloadableMetadata.getMissingRange();
        int tmpNumberOfWorkers = numberOfWorkers;
        while ( missingRange != null) {
            if (missingRange.getLength() / chunksize < numberOfWorkers - 1){
                tmpNumberOfWorkers = 1;
            }
            numOfChunks = (int) Math.ceil(missingRange.getLength() / (double) chunksize);
            int chunksPerThread = numOfChunks / tmpNumberOfWorkers;
            ExecutorService executor = Executors.newFixedThreadPool(tmpNumberOfWorkers);
            HTTPRangeGetter[] httpRangeGetters = new HTTPRangeGetter[tmpNumberOfWorkers];
            long start = missingRange.getStart();
            for (int i = 0; i < tmpNumberOfWorkers; i++) {
                long end = start + (chunksPerThread * chunksize) - 1;
                if (i == numberOfWorkers - 1) {
                    end = fileSize - 1;
                }
                httpRangeGetters[i] = new HTTPRangeGetter(url, new Range(start, end), chunkQueue, tokenBucket, limitDownload, fileSize);
                executor.execute(httpRangeGetters[i]);
                start = end + 1;
            }
            executor.shutdown();
            while (! executor.isTerminated()) {
            }
            missingRange = downloadableMetadata.getMissingRange();
            tmpNumberOfWorkers = numberOfWorkers;
        }
        while (! chunkQueue.isEmpty()) {
        }
        if (tokenBucket != null) {
            tokenBucket.terminate();
        }
        fileWriter.interrupt();//TODO maybe not the best way
        try {
            fileWriter.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        downloadableMetadata.delete();
        //TODO
    }
    public static long getFileSize(String fileUrl){
        long fileSize = 0;
        try {
            URL url = new URL(fileUrl);
            HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
            // retrieve file size from Content-Length header field
            fileSize = Long.parseLong(httpURLConnection.getHeaderField("Content-Length"));
        } catch (Exception e) {
            e.printStackTrace(); //TODO error handle
        }
        return fileSize;
    }
}
