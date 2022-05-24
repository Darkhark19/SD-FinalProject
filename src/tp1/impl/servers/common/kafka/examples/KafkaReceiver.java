package tp1.impl.servers.common.kafka.examples;

import java.util.List;

import tp1.impl.servers.common.kafka.KafkaSubscriber;

public class KafkaReceiver {
	private static final String FROM_BEGINNING = "earliest";

	public static void main(String[] args) {

		var subscriber = KafkaSubscriber.createSubscriber(KafkaSender.KAFKA_BROKERS, List.of(KafkaSender.TOPIC),
				FROM_BEGINNING);

		subscriber.start(true, (r) -> {
			System.out.printf("SeqN: %s %d %s\n", r.topic(), r.offset(), r.value());
		});
	}
}
