package cumt.tj.learn;

import java.io.File;

/**
 * Created by sky on 17-5-15.
 */
public class FileUtil {

    //删除output文件夹，这样就不用手动删除了
    public static boolean deleteDir(String path) {
        File dir = new File(path);
        if (dir.exists()) {
            for (File f : dir.listFiles()) {
                if (f.isDirectory()) {
                    deleteDir(f.getName());
                } else {
                    f.delete();
                }
            }
            dir.delete();
            return true;
        } else {
            System.out.println("文件(夹)不存在!");
            return false;
        }
    }
}
