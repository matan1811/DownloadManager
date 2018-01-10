import java.io.Serializable;

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
    private String url;
    private Boolean[] chunkArray;

    DownloadableMetadata(String url, int fileSize) {
        this.url = url;
        this.filename = getName(url);
        this.metadataFilename = getMetadataName(filename);
        this.chunkArray = new Boolean[fileSize / HTTPRangeGetter.CHUNK_SIZE];
    }

    private static String getMetadataName(String filename) {
        return filename + ".metadata";
    }

    private static String getName(String path) {
        return path.substring(path.lastIndexOf('/') + 1, path.length());
    }

    void addRange(Range range) {
        int location = (int) (Math.ceil(range.getEnd() / HTTPRangeGetter.CHUNK_SIZE)) - 1;
        chunkArray[location] = true;
    }

    String getFilename() {
        return filename;
    }

    boolean isCompleted() {
        for (Boolean chunk : chunkArray){
            if (chunk == false) return false;
        }
        return true;
    }

    void delete() {
        //TODO
    }

    Range getMissingRange() {
        for (int i = 0 ; i < chunkArray.length ; i++){
            if (chunkArray[i] == false){
                long end = ((i + 1) * 1024) - 1;
                return new Range(end - HTTPRangeGetter.CHUNK_SIZE ,end);
            }
        }
        return null;
    }

    String getUrl() {
        return url;
    }
}
