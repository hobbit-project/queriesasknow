package org.hobbit.benchmark.questionanswering.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.hobbit.QaldBuilder;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummySystemAdapter extends AbstractSystemAdapter {
	private static final Logger LOGGER = LoggerFactory.getLogger(DummySystemAdapter.class);
	private static final String SPARQL_SERVICE = "http://local-dbpedia:8890/sparql";
	
	public DummySystemAdapter() {
		LOGGER.info("QaDummySys: Constructed.");
	}

	@Override
	public void init() throws Exception {

		LOGGER.info("QaDummySys: Initializing - loading correct answers ...");
		
		LOGGER.info("QaDummySys: Initializing - correct answers loaded...");
		super.init();
		LOGGER.info("QaDummySys: Initialized.");
	}

	public void receiveGeneratedData(byte[] data) {
		// Nothing to handle here.
		LOGGER.info("QaDummySys: Data received. Oops.");
	}

	public void receiveGeneratedTask(String taskIdString, byte[] data) {
		LOGGER.info("QaDummySys: Task " + taskIdString + " received.");
		
		/** START - GET INFO from task data **/
		String taskAsString = RabbitMQUtils.readString(data);
		QaldBuilder qaldBuilder = new QaldBuilder(taskAsString);
		try {
			HttpURLConnection httpcon = (HttpURLConnection) (new URL("https://asknowdemo.sda.tech/earl/api/answerdetail").openConnection());
			httpcon.setDoOutput(true);
			httpcon.setRequestProperty("Content-Type", "application/json");
			httpcon.setRequestMethod("POST");
			httpcon.connect();
			//byte[] outputBytes = "{\"nlquery\":\"Where was Angela Merkel born\", \"pagerankflag\":true}".getBytes("UTF-8");
			byte[] outputBytes = ("{\"nlquery\":\""+qaldBuilder.getQuestionString()+"\", \"pagerankflag\":true}").getBytes("UTF-8");
			OutputStream os = httpcon.getOutputStream();
			os.write(outputBytes);
			os.close();
			BufferedReader inreader = new BufferedReader(new InputStreamReader(httpcon.getInputStream()));
			String decodedMsg = inreader.readLine();
			LOGGER.info("QaDummySys: Query -> "+qaldBuilder.getQuestionString());
			if(checkQuery(decodedMsg)) {
				JsonObject json = JSON.parse(decodedMsg);
				String query = json.get("sparql").getAsObject().get("queries").getAsArray().get(0).toString();
				qaldBuilder.setQuery(query);
				LOGGER.info("QaDummySys: Query -> "+query);
				//qaldBuilder.setAnswers(SPARQL_SERVICE);
			}
	        inreader.close();
	        httpcon.disconnect();
		}catch(Exception e) {
			LOGGER.error("QaDummySys: " + e.getMessage());
		}
		String qaldAnswer = qaldBuilder.getQaldQuestion().toString();
		byte[] sendData = RabbitMQUtils.writeString(qaldAnswer);
		try {
			sendResultToEvalStorage(taskIdString, sendData);
			// LOGGER.info("QaDummySys: Data has been sent to evaluation model");
		} catch (IOException e) {
			LOGGER.error("QaDummySys: " + e.getMessage());
		}
		
		/** END - PREPARING AND SENDING answer **/
	}

	@Override
	public void close() throws IOException {
		LOGGER.info("QaDummySys: Closing.");
		super.close();
		LOGGER.info("QaDummySys: Closed.");
	}
	public boolean checkQuery(String json) {
		try {
			JsonObject query = JSON.parse(json);
			query = query.get("sparql").getAsObject();
			if(!query.hasKey("queries"))
				return false;
			if(query.get("queries").getAsArray().size()<1)
				return false;
		}catch(Exception e) {
			return false;
		}
		return true;
	}
}