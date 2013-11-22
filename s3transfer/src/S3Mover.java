import java.util.Map;

import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;


public  class S3Mover implements Runnable, Mover {

	MoverData data = new MoverData();
	private String sourceBucket;
	private String destBucket;
	private Boolean md5check;
	S3Mover(final S3Service restService, final S3Object sourceObject, final String sourceObjectKey) {

		this.data.restService = restService;
		this.data.sourceObject = sourceObject;
		this.data.sourceObjectKey = sourceObjectKey;
	}
	
	public void checkMd5()
	{
		this.md5check = true;
	}

	public void setSourceBucket(String sourceBucket) {
		this.sourceBucket = sourceBucket;
	}

	public void setDestBucket(String destBucket) {
		this.destBucket = destBucket;
	}

	public void run() {

		Map moveResult = null;
		try {

			moveResult = data.restService.moveObject(this.sourceBucket, data.sourceObjectKey, this.destBucket, data.sourceObject, false);
			if (moveResult.containsKey("DeleteException")) {

				System.out.println("Error: " + data.sourceObjectKey);

			}

		} catch (S3ServiceException e) {
			System.out.println("Error: " + data.sourceObjectKey + " EXCEPTION: " + e.getMessage());

		} catch (ServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}