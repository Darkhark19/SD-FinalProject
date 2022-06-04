package tp1.impl.servers.rest;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import com.google.gson.Gson;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import tp1.api.FileInfo;
import tp1.api.service.java.Files;
import tp1.api.service.java.Result;
import tp1.api.service.rest.RestFiles;
import tp1.impl.servers.common.JavaFiles;
import tp1.impl.servers.common.kafka.KafkaSubscriber;
import tp1.impl.servers.common.kafka.RecordProcessor;
import tp1.impl.servers.common.kafka.RepFiles;
import tp1.impl.servers.common.kafka.sync.SyncPoint;

@Singleton
public class FilesResources extends RestResource implements RestFiles {
	private static Logger Log = Logger.getLogger(FilesResources.class.getName());

	private static final String WRITE = "write";
	private static final String DELETE = "delete";
	private static final String GET = "get";
	private static final String DELETES = "deletes";
	static final String KAFKA_BROKERS = "localhost:9092";
	static final String FROM_BEGINNING = "earliest";
	public static final String TOPIC = "files";

	final Files receiverImpl;
	final KafkaSubscriber receiver;
	final SyncPoint<Object> sync = SyncPoint.getInstance();

	final Gson json;

	public FilesResources() {
		json = new Gson();
		receiverImpl = new JavaFiles();
		List<String> topic = new LinkedList<>();
		topic.add(TOPIC);
		this.receiver = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, topic, FROM_BEGINNING);
		this.receiver.start(false, new RecordProcessor() {
			public void onReceive(ConsumerRecord<String, String> r) {

				String key = r.key();
				String val = r.value();
				Object[] op = json.fromJson(val, Object[].class);
				String fileId;
				String token;
				switch (key){
					case DELETE :
						fileId = op[0].toString();
						token = op[1].toString();
						Result<Void> delete = receiverImpl.deleteFile(fileId, token);
						sync.setResult(r.offset(), delete);
						break;
					case DELETES:
						token = op[1].toString();
						Result<Void> deletes = receiverImpl.deleteUserFiles(op[0].toString(), token);
						sync.setResult(r.offset(), deletes);
						break;
					default:
						break;
				}


			}
		});
	}

	@Override
	public void writeFile(String fileId, byte[] data, String token) {
		//Log.info(String.format("REST writeFile: fileId = %s, data.length = %d, token = %s \n", fileId, data.length, token));
		super.resultOrThrow( receiverImpl.writeFile(fileId, data, token));
	}

	@Override
	public void deleteFile(String fileId, String token) {
		//Log.info(String.format("REST deleteFile: fileId = %s, token = %s \n", fileId, token));

		super.resultOrThrow( receiverImpl.deleteFile(fileId, token));
	}

	@Override
	public byte[] getFile(String fileId, String token) {
		Log.info(String.format("REST getFile: fileId = %s,  token = %s \n", fileId, token));

		return resultOrThrow( receiverImpl.getFile(fileId, token));
	}

	@Override
	public void deleteUserFiles(String userId, String token) {
		//Log.info(String.format("REST deleteUserFiles: userId = %s, token = %s \n", userId, token));

		super.resultOrThrow( receiverImpl.deleteUserFiles(userId, token));
	}
/*
	@Override
	public void writeFile(Long version, String fileId, byte[] data, String token) {

	}

	@Override
	public void deleteFile(Long version, String fileId, String token) {

	}

	@Override
	public byte[] getFile(Long version, String fileId, String token) {
		return new byte[0];
	}

	@Override
	public void deleteUserFiles(Long version, String userId, String token) {
		super.resultOrThrow( impl.deleteUserFiles(userId, token));
	}*/
}
