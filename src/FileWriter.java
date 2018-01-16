import java.io.*;
import java.nio.file.Files;
import java.util.concurrent.BlockingQueue;

/**
 * This class takes chunks from the queue, writes them to disk and updates the file's metadata.
 *
 * NOTE: make sure that the file interface you choose writes every update to the file's content or metadata
 *       synchronously to the underlying storage device.
 */
public class FileWriter implements Runnable {

    private final BlockingQueue<Chunk> chunkQueue;
    private DownloadableMetadata downloadableMetadata;
    String filename;



    FileWriter(DownloadableMetadata downloadableMetadata, BlockingQueue<Chunk> chunkQueue) {
        this.chunkQueue = chunkQueue;
        this.downloadableMetadata = downloadableMetadata;
        this.filename = filename;
    }

    private void writeChunks() throws IOException {
        while (true) {
            Chunk chunk = chunkQueue.poll();
            if (chunk != null) {
//                File file = new File("C:\\Users\\matan\\Google Drive\\CS2015_6\\Year3\\net\\DownloaManager\\" + downloadableMetadata.getFilename());
                File file = new File(downloadableMetadata.getFilename());
//                FileOutputStream tempMetadata = new FileOutputStream("C:\\Users\\matan\\Google Drive\\CS2015_6\\Year3\\net\\DownloaManager\\" + downloadableMetadata.getMetadataFilename() + ".tmp");
                FileOutputStream tempMetadata = new FileOutputStream(downloadableMetadata.getMetadataFilename() + ".tmp");
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rws");
                ObjectOutputStream tmpObjectOutputStream = new ObjectOutputStream(tempMetadata);
                randomAccessFile.seek(chunk.getOffset());
                randomAccessFile.write(chunk.getData());
                randomAccessFile.close();
                downloadableMetadata.addRange(new Range(chunk.getOffset(), chunk.getOffset() + HTTPRangeGetter.CHUNK_SIZE));
                tmpObjectOutputStream.writeObject(downloadableMetadata.getChunkArray());
                tmpObjectOutputStream.close();
//                File metaFile = new File("C:\\Users\\matan\\Google Drive\\CS2015_6\\Year3\\net\\DownloaManager\\" + downloadableMetadata.getMetadataFilename());
                File metaFile = new File(downloadableMetadata.getMetadataFilename());
//                File tmpFile = new File("C:\\Users\\matan\\Google Drive\\CS2015_6\\Year3\\net\\DownloaManager\\" + downloadableMetadata.getMetadataFilename() + ".tmp");
                File tmpFile = new File(downloadableMetadata.getMetadataFilename() + ".tmp");
                if (metaFile.exists()) {
                    metaFile.delete();
                }
                tmpFile.renameTo(metaFile);
            }
            if (Thread.interrupted()) { //TODO is it the best way?
                break;
            }
        }
    }

    @Override
    public void run() {
        try {
            this.writeChunks();
        } catch (IOException e) {
            e.printStackTrace();
            //TODO
        }
    }
}
