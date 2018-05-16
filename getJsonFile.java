import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class getJsonFile {
	public static void getJson(String input,ObjectMapper mapper){
		try {
			JsonNode node = mapper.readTree(input);
			System.out.println(node.toString());
		}catch (JsonProcessingException e) {
			e.printStackTrace();
			// TODO: handle exception
		} 
		catch (IOException e) {
			e.printStackTrace();
			// TODO: handle exception
		}
	}
    public static void jsonLeaf(JsonNode node)  
    {  
        if (node.isValueNode() || node.isContainerNode())  
        {  
            System.out.printf("[%s]",node.toString());  
            return;  
        }  
      
        if (node.isObject())  
        {  
            Iterator<Entry<String, JsonNode>> it = node.fields();  
            while (it.hasNext())  
            {  
                Entry<String, JsonNode> entry = it.next();  
                jsonLeaf(entry.getValue());  
            }  
        }  
      
        if (node.isArray())  
        {  
            Iterator<JsonNode> it = node.iterator();  
            while (it.hasNext())  
            {  
                jsonLeaf(it.next());  
            }  
        }  
    } 
    public static void readFileByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            ObjectMapper mapper = new ObjectMapper();
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
            	System.out.println(line);
                getJson(tempString,mapper);
                line++;
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
	public static void main(String[] args)throws Exception{
		String path = "/home/hadoop/Desktop/input.log";
		readFileByLines(path);
		System.exit(0);
	}
}
