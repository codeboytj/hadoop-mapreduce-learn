package cumt.tj.learn.classification;

import org.junit.Test;

import java.io.IOException;

/**
 * Created by sky on 17-5-19.
 */
public class LogisticPredExampleTest {
    @Test
    public void getData(){
        DataPrepare dataPrepare =new DataPrepare();
        try {
            dataPrepare.getData();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
