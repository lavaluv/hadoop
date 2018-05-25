import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgress {
	public void fileCopy(String filePath,String dst) throws IOException {
		InputStream in = new BufferedInputStream(new FileInputStream(filePath));
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst),conf);
		//create() will create all dirs
		OutputStream out = fs.create(new Path(dst),new Progressable() {
			public void progress() {
				System.out.println("64KB:File copy from " + filePath + " to "+dst);
			}
		});
		IOUtils.copyBytes(in, out, 4096,true);
	}
	public void loadFiles(File[] files,String dst) throws IOException, InterruptedException{
		for(int index = 0; index < files.length; index++) {
			if (files[index].isFile()) {
				fileCopy(files[index].getPath(),dst + files[index].getName());
			}else if(files[index].isDirectory()){
				File newFile = new File(files[index].getPath());
				File[] dir = newFile.listFiles();
				loadFiles(dir,dst);
			}
		}
	}
	public static void main(String[] args)throws Exception{
		String localSrc = args[0];
		String dst = args[1];
		if (dst.substring(dst.length()-1) != "/") {
			dst = dst + "/";
		}
		FileCopyWithProgress fs = new FileCopyWithProgress();
		File file = new File(localSrc);
		if (file.isFile()) {
			fs.fileCopy(localSrc, dst + file.getName());
		}
		else {
			File[] files = file.listFiles();
			fs.loadFiles(files, dst);
		}
	}
}
