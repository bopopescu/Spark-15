import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang3.StringEscapeUtils;
import org.json.JSONException;
import org.json.JSONObject;



public class MyMapper {
	public static void main (String args[])
	{
		try{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("tweetsample.json")));
			String input;
			File file = new File("output");
			if(!file.exists())
			{
				file.createNewFile();
			}
			FileOutputStream out = new FileOutputStream(file);
			PrintStream p = new PrintStream(out);
			while((input = br.readLine())!=null)
			{
				try
				{
					JSONObject json= new JSONObject(input);
					String time = (String)json.get("created_at");
					String [] timesplit = time.split("\\s+");
					//yyyy MM  dd HH:mm:ss
					StringBuilder sb = new StringBuilder(timesplit[5]+" "+timesplit[1]+" "+timesplit[2]+" "+timesplit[3]);
					SimpleDateFormat df = new SimpleDateFormat("yyyy MMM dd HH:mm:ss");
					Date date = df.parse(sb.toString());
					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					
					String timestamp = formatter.format(date);
					JSONObject user = json.getJSONObject("user");
					String userid = (String) user.get("id_str");
					String text = (String)json.getString("text");
					String tweetid = (String)json.getString("id_str");
					//StringBuilder mytext = new StringBuilder(unescape_perl_string(text));
					//System.out.println(tweetid+","+userid+","+timestamp+","+"\""+removeUnicode(text)+"\"");
					p.println(tweetid+","+userid+","+timestamp+","+"\""+removeUnicode(text)+"\"");
				}
				catch(Exception e)
				{
					continue;
				}
			}
		}
		catch(IOException io)
		{
			io.printStackTrace();
		} 
	}
	public static String removeUnicode(String input){
	    StringBuffer buffer = new StringBuffer(input.length());
	    for (int i =0; i < input.length(); i++)
	    {
	    	if ( input.charAt(i) == '\n'){
	    		buffer.append("\\\\n");
	    		}
	    	else if ( input.charAt(i) == '\t'){
	    		buffer.append("\\\\t");
	    		}
	    	else 
	    	{
	    		buffer.append(input.charAt(i));
	    	}
	    }
	    return buffer.toString();
	}
}
