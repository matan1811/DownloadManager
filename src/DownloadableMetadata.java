import java.io.*;
import java.util.Arrays;

/**
 * Describes a file's metadata: URL, file name, size, and which parts already downloaded to disk.
 *
 * The metadata (or at least which parts already downloaded to disk) is constantly stored safely in disk.
 * When constructing a new metadata object, we first check the disk to load existing metadata.
 *
 * CHALLENGE: try to avoid metadata disk footprint of O(n) in the average case
 * HINT: avoid the obvious bitmap solution, and think about ranges...
 */
class DownloadableMetadata implements Serializable {
    private final String metadataFilename;
    private String filename;
    //private String url;
    private boolean[] chunkArray;
//    private int lastAccess;
//    private int first;
//    private int laste;
    int lastStart;
    int chunkSize;
    long fileSize;

    DownloadableMetadata(String url, long fileSize) throws IOException, ClassNotFoundException {
        //this.url = url;
        this.fileSize = fileSize;
        this.filename = getName(url);
        this.metadataFilename = getMetadataName(filename);
        chunkSize = HTTPRangeGetter.CHUNK_SIZE;
//        File file = new File("C:\\Users\\matan\\Google Drive\\CS2015_6\\Year3\\net\\DownloaManager\\" + metadataFilename);
        File file = new File(metadataFilename);
        File tmpFile = new File(metadataFilename + ".tmp");
        if (file.exists()) {
//            ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream("C:\\Users\\matan\\Google Drive\\CS2015_6\\Year3\\net\\DownloaManager\\" + metadataFilename));
            ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(metadataFilename));
            boolean[] oldArray = (boolean[]) objectInputStream.readObject();
            this.chunkArray = oldArray;
        } else if (tmpFile.exists()){
            ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(metadataFilename + ".tmp"));
            boolean[] oldArray = (boolean[]) objectInputStream.readObject();
            this.chunkArray = oldArray;
        } else {
            this.chunkArray = new boolean[(int) Math.ceil(fileSize / (double) chunkSize)];
            Arrays.fill(this.chunkArray,false);
        }
        lastStart = 0;
    }

    private static String getMetadataName(String filename) {
        return filename + ".metadata";
    }

    private static String getName(String path) {
        return path.substring(path.lastIndexOf('/') + 1, path.length());
    }

    void addRange(Range range) {
        int location = (int) (Math.ceil(range.getEnd() / (double) chunkSize)) - 1;
        chunkArray[location] = true;
    }

    String getFilename() {
        return filename;
    }

    String getMetadataFilename() {
        return metadataFilename;
    }

    boolean[] getChunkArray(){
        return chunkArray;
    }
//    boolean isCompleted() {
//        for (Boolean chunk : chunkArray) {
//            if (chunk == false) return false;
//        }
//        return true;
//    }

    void delete() {
//        File file = new File("C:\\Users\\matan\\Google Drive\\CS2015_6\\Year3\\net\\DownloaManager\\" + metadataFilename);
        File file = new File(metadataFilename);
        File tmpFile = new File(metadataFilename);
        file.delete();
        tmpFile.delete();
    }
    synchronized Range getMissingRange() {
        boolean firstMissing = true;
        long rangeStart = 0, rangeEnd = 0;
        for (int i = lastStart; i < chunkArray.length; i++) {
            if (chunkArray[i] == false && firstMissing) {
                rangeStart = (i * chunkSize);
                rangeEnd = rangeStart + chunkSize;
                firstMissing = false;
            }else if (i == chunkArray.length - 1){
                lastStart = i + 1;
                return new Range(rangeStart, fileSize - 1);
            } else if ((chunkArray[i] == true && !firstMissing)) {
                lastStart = i + 1;
                return new Range(rangeStart, rangeEnd - 1);
            } else if (chunkArray[i] == false && !firstMissing) {
                rangeEnd += chunkSize;
            }
        }
        return null;
    }
//    String getUrl() {
//        return url;
//    }
}