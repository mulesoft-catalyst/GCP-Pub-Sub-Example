package google.subscriber;

import java.io.FileInputStream;
import java.util.Iterator;
import java.util.Map;

import org.mule.api.MuleEventContext;
import org.mule.api.lifecycle.Callable;
import org.mule.api.transport.PropertyScope;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

/**
 * create a Pub/Sub pull subscription and
 * asynchronously pull messages from it.
 */
public class SubscriberTopic implements Callable{

  /** Receive messages over a subscription. */
  public void subscribeMsgTransaction(MuleEventContext eventContext) throws Exception {
    
    String subscription_id= eventContext.getMessage().getProperty("subscription_id",PropertyScope.INVOCATION);
	String project_id= eventContext.getMessage().getProperty("project",PropertyScope.INVOCATION);
	String key_file_name= eventContext.getMessage().getProperty("keyFile",PropertyScope.INVOCATION);
    String basePath = determineBasePath(eventContext);
    String json_key_file=basePath + "/" + key_file_name;
    
    GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(json_key_file));
    ProjectSubscriptionName subscription = ProjectSubscriptionName.of(project_id, subscription_id);
    
    MessageReceiver receiver =
				new MessageReceiver() {
					@Override
					public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
						 System.err.println(message.getData().toStringUtf8());
						consumer.ack();

					}
				};
				
				
				 Subscriber subscriber = null;
				    try {
				    	
				      subscriber = Subscriber.newBuilder(subscription, receiver).setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
				       subscriber.addListener(
				          new Subscriber.Listener() {
				            @Override
				            public void failed(Subscriber.State from, Throwable failure) {
				     
				              System.err.println(failure);
				            }
				          },
				          MoreExecutors.directExecutor());
				      subscriber.startAsync().awaitRunning();
				      subscriber.awaitTerminated();
			      
				    } finally {
				      if (subscriber != null) {
				        subscriber.stopAsync();
				      }
				    }
    
    
  }
  
  public String determineBasePath(MuleEventContext eventContext) {
		
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

  
  @Override
  public Object onCall(MuleEventContext eventContext) throws Exception {
	  subscribeMsgTransaction(eventContext);
	  return eventContext;
  }
}