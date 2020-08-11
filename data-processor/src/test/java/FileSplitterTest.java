import com.outliers.FileSplitter;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class FileSplitterTest {

    @Test
    void getFilePath() {
        File fileToWrite = new File(System.getProperty("java.io.tmpdir")+"testDirsensor1/uploadTestFile.csv");
        try {
            new File(System.getProperty("java.io.tmpdir")+"testDirsensor1").mkdir();
            fileToWrite.createNewFile();
            fileToWrite.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String resultPath = FileSplitter.getFilePath("testDir", "sensor1");
        assertEquals(resultPath, fileToWrite.toString());

    }
}