package net.wessendorf.amqp;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSender;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;

import java.util.logging.Logger;

import static org.apache.qpid.proton.Proton.message;

public class MyQpidApp extends AbstractVerticle {

    private static final Logger LOGGER = Logger.getLogger(MyQpidApp.class.getName());

    @Override
    public void start(Future<Void> future) {


        final ProtonClient client = ProtonClient.create(vertx);

        // Connect, then use the event loop thread to process things thereafter
        client.connect("172.17.0.3", 5672, connectResult -> {
            if (connectResult.succeeded()) {

                LOGGER.info("connectd");

                connectResult.result().setContainer("my-container/client-id").openHandler(openResult -> {
                    if (openResult.succeeded()) {
                        ProtonConnection connection = openResult.result();
                        // Create senders, receivers etc..
                        //send(connection);

                        connection.createSender("myQueue").openHandler(openSenderResult -> {
                            if (openSenderResult.succeeded()) {
                                ProtonSender sender = openSenderResult.result();

                                Message message = message();
                                message.setBody(new AmqpValue("Hello World"));

                                // Send message, providing an onUpdated delivery handler that prints updates
                                sender.send(message, delivery -> {
                                    System.out.println(String.format("Message received by server: remote state=%s, remotely settled=%s",
                                            delivery.getRemoteState(), delivery.remotelySettled()));
                                });
                            }
                        }).open();

                    }
                }).open();
            }
        });
    }

}
