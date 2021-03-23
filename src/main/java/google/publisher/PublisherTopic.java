package google.publisher;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.api.MuleEventContext;
import org.mule.api.lifecycle.Callable;
import org.mule.api.transport.PropertyScope;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;


public class PublisherTopic implements Callable {
	  
	  @Override
	public Object onCall(MuleEventContext eventContext) throws Exception {

		sendToTopic(eventContext);
		return eventContext;

	} 

	void sendToTopic(MuleEventContext eventContext) throws Exception {
		
		Log logger = LogFactory.getLog(getClass());	
		
		String project_id= eventContext.getMessage().getProperty("project",PropertyScope.INVOCATION);
		String key_file_name= eventContext.getMessage().getProperty("keyFile",PropertyScope.INVOCATION);
		String topic_id= eventContext.getMessage().getProperty("topic-id",PropertyScope.INVOCATION);
		
		System.out.println("project_id is ->"+project_id);
        System.out.println("topic_id is ->"+topic_id);

		ProjectTopicName topicName = ProjectTopicName.of(project_id, topic_id);
		String basePath = determineBasePath(eventContext);
		String json_key_file=basePath + "/" + key_file_name;

		System.out.println("json_key_file :" + json_key_file);

		GoogleCredentials credentials = GoogleCredentials
				.fromStream(new FileInputStream(json_key_file));
		Publisher publisher = Publisher.newBuilder(topicName)
				.setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();

		List<ApiFuture<String>> futures = new ArrayList<>();

		try {

			String message = (String)eventContext.getMessage().getPayload();
			eventContext.getMessage().setProperty("outgoing_payload", message, PropertyScope.INVOCATION);
			
			ByteString data = ByteString.copyFromUtf8(message);
			PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
			ApiFuture<String> future = publisher.publish(pubsubMessage);
			futures.add(future);

		} finally {
			// Wait on any pending requests
			List<String> messageIds = ApiFutures.allAsList(futures).get();

			for (String messageId : messageIds) {
				eventContext.getMessage().setProperty("test_id", messageId, PropertyScope.INVOCATION);
			}

			if (publisher != null) {
				//shutdown to free up resources.
				publisher.shutdown();
				publisher.awaitTermination(1, TimeUnit.MINUTES);
			}
		}
	}


	public String determineBasePath(MuleEventContext eventContext) {
		// logic to resolve server level folder path issues
		String HOME = "${app.home}";
		String muleHome = eventContext.getMuleContext().getRegistry().get("app.home");

		String basePath = eventContext.getMuleContext().getRegistry().get("cert.path");

		if (org.mule.util.StringUtils.isEmpty(basePath)) {
			basePath = muleHome + "/classes";

		} else {
			if (basePath.contains(HOME)) {
				basePath = basePath.replace(HOME, muleHome);

			}
		}
		return basePath;
	}

}
