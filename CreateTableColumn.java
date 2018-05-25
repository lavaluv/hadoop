import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTableColumn {
	//use one line of json file'keys to create HBase's columns
	//exmple:{"key":"value"} => HBase:{NAME => {key}
	private void addColumn(File file) throws IOException,InterruptedException{
        BufferedReader reader = null;
		String input = "";
		try {
			System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
            // 一次读入一行，直到读入null为文件结束
            while((input = reader.readLine()) != null) {
            	//get HBase table by name
        		Table table = connection.getTable(TableName.valueOf("pkg"));
        		TableDescriptor tableDescriptor = table.getDescriptor();
        		//get table column
        		Set<byte[]> columnName = tableDescriptor.getColumnFamilyNames();
        		Iterator<byte[]> iterator = columnName.iterator();
               	Map<?, ?> pkg = RecordParser.parse(input);
               	//create every column which not exist
           		for(Object key:pkg.keySet()) {
           			boolean columnExist = false;
           			while(iterator.hasNext()) {
           				String string = new String(iterator.next());
           				if (key.toString().equalsIgnoreCase(string)) {
           					columnExist = true;
           					break;
           				}
           			}
               		if (!columnExist) {
               			Admin admin = connection.getAdmin();
               			ColumnFamilyDescriptor columnDescriptor = 
               					ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(key.toString())).build();
               			admin.addColumnFamily(TableName.valueOf("pkg"), columnDescriptor);
               			System.out.println("Add column " + key.toString());
               		}
           		}
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
	}
	//load all files in the given dir
	public void loadFile(File[] files) throws IOException, InterruptedException{
		for(int index = 0; index < files.length; index++) {
			if (files[index].isFile()) {
				addColumn(files[index]);
			}else if(files[index].isDirectory()){
				File newFile = new File(files[index].getPath());
				File[] dir = newFile.listFiles();
				loadFile(dir);
			}
		}
	}
	public static void main(String[] args) throws Exception{
        File file = new File("/home/hadoop/Desktop/f");
        File[] files = file.listFiles();
        System.out.println(files.length);
        CreateTableColumn column = new CreateTableColumn();
        column.loadFile(files);
	}
}
