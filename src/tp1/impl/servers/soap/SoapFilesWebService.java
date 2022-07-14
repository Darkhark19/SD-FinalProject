package tp1.impl.servers.soap;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import com.google.gson.Gson;
import jakarta.jws.WebService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import tp1.api.service.java.Files;
import tp1.api.service.java.Result;
import tp1.api.service.soap.FilesException;
import tp1.api.service.soap.SoapFiles;
import tp1.impl.servers.common.JavaFiles;
import tp1.impl.servers.common.dropbox.DropBoxFiles;
import tp1.impl.servers.common.kafka.KafkaSubscriber;
import tp1.impl.servers.common.kafka.RecordProcessor;
import tp1.impl.servers.common.kafka.sync.SyncPoint;

@WebService(serviceName = SoapFiles.NAME, targetNamespace = SoapFiles.NAMESPACE, endpointInterface = SoapFiles.INTERFACE)
public class SoapFilesWebService extends SoapWebService implements SoapFiles {

	private static Logger Log = Logger.getLogger(SoapFilesWebService.class.getName());
	private static final String WRITE = "write";
	private static final String DELETE = "delete";
	private static final String GET = "get";
	private static final String DELETES = "deletes";
	static final String KAFKA_BROKERS = "kafka:9092";
	final Files receiverImpl;
	public static final String TOPIC = "files";
	final KafkaSubscriber receiver;
	final SyncPoint<Object> sync = SyncPoint.getInstance();
	static final String FROM_BEGINNING = "earliest";
	final Gson json;
	
	public SoapFilesWebService() {
		receiverImpl = new JavaFiles();
		json = new Gson();
		List<String> topic = new LinkedList<>();
		topic.add(TOPIC);
		this.receiver = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, topic,FROM_BEGINNING);
		this.receiver.start(false, new RecordProcessor() {
			@Override
			public void onReceive(ConsumerRecord<String, String> r) {

				String key = r.key();
				String val = r.value();
				Object[] op = json.fromJson(val,Object[].class);
				String fileId;
				byte[] data;
				String token;
				switch (key) {
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
					default :
						break;
				}
			}
		});
	}


	@Override
	public void writeFile(String fileId, byte[] data, String token) throws FilesException {
		Log.info(String.format("SOAP writeFile: fileId = %s, data.length = %d, token = %s \n", fileId, data.length, token));

		super.resultOrThrow( receiverImpl.writeFile(fileId, data, token), FilesException::new );
	}
	
	@Override
	public void deleteFile(String fileId, String token) throws FilesException {
		Log.info(String.format("SOAP deleteFile: fileId = %s, token = %s \n", fileId, token));

		super.resultOrThrow( receiverImpl.deleteFile(fileId, token), FilesException::new);
	}
	
	@Override
	public byte[] getFile(String fileId, String token) throws FilesException {
		Log.info(String.format("SOAP getFile: fileId = %s,  token = %s \n", fileId, token));

		return super.resultOrThrow( receiverImpl.getFile(fileId, token), FilesException::new);
	}

	@Override
	public void deleteUserFiles(String userId, String token) throws FilesException {
		Log.info(String.format("SOAP deleteUserFiles: userId = %s, token = %s \n", userId, token));

		super.resultOrThrow( receiverImpl.deleteUserFiles(userId, token), FilesException::new);
	}
}
