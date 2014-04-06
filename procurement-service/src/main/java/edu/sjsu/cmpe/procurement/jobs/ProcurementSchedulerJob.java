package edu.sjsu.cmpe.procurement.jobs;

import java.util.ArrayList;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.eclipse.jetty.util.ajax.JSONDateConvertor;
import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;
import edu.sjsu.cmpe.procurement.ProcurementService;
import edu.sjsu.cmpe.procurement.domain.Book;
/**
 * This job will run at every 5 second.
 */
@Every("5s")
public class ProcurementSchedulerJob extends Job {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void doJob(){

		try {

			ArrayList<Long> longArray = new ArrayList<Long>();
			longArray = consumeQueueMessages();
			log.info("Messages from Queue = {}", longArray.size());
			if (!longArray.isEmpty()) {
				//PostMessagesToPublisher(longArray);
				//getMessagesFromPublisher();
			}

		} catch (Exception e) {
			// System.out.println(e.printStackTrace());
		}
		String strResponse = ProcurementService.jerseyClient.resource(
				"http://ip.jsontest.com/").get(String.class);
		log.debug("Response from jsontest.com: {}", strResponse);

	}
    
    public ArrayList<Long> consumeQueueMessages() throws JMSException {
		String user = "admin";
		String password = "password";
		String host = "54.215.133.131";
		int port = 61613;

		ArrayList<Long> isbnArray = new ArrayList<Long>();

		int i = 0;
		String queue = "/queue/68571.book.orders";
		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + host + ":" + port);

		Connection connection = factory.createConnection(user, password);
		connection.start();
		Session session = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		Destination dest = new StompJmsDestination(queue);

		MessageConsumer consumer = session.createConsumer(dest);
		System.out.println("Waiting for messages from " + queue + "...");

		long waitUntil = 5000; // wait for 5 sec

		while (true) {
			Message msg = consumer.receive(waitUntil);
			if (msg == null) {
				System.out.println("No new messages. Existing due to timeout - "+ waitUntil / 1000 + " sec");
				break;
			}
			if (msg instanceof TextMessage) {
				String body = ((TextMessage) msg).getText();
				System.out.println("Received message = " + body);
				String isbnSplitArray[] = body.split(":");
				Long isbnValue = Long.parseLong(isbnSplitArray[1]);
				System.out.println(isbnValue);
				isbnArray.add(i, isbnValue);
				i++;

			} 
			
			else {
				System.out.println("Unexpected message type: " + msg.getClass());
			}
		} // end while loop
		connection.close();
		System.out.println("Done");
		return isbnArray;
	}
    
}
