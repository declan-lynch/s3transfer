import org.jets3t.service.S3Service;
import org.jets3t.service.model.S3Object;


public class MoverData {
	public S3Service restService;
	public S3Object sourceObject;
	public String sourceObjectKey;

	public MoverData() {
	}
}