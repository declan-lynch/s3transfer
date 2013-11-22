import java.util.Map;

import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;


public  class S3CopyMover implements Runnable, Mover {

	MoverData data = new MoverData();
	private String sourceBucket;
	private String destBucket;
	private Boolean md5check;
	private String sourcePrefix;
	private String destPrefix;
	private String destinationKey;
	private S3Object destinationObject;
	
	S3CopyMover(final S3Service restService, final S3Object sourceObject, final String sourceObjectKey) {

		this.data.restService = restService;
		this.data.sourceObject = sourceObject;
		this.data.sourceObjectKey = sourceObjectKey;
	}

	public void setSourceBucket(String sourceBucket) {
		this.sourceBucket = sourceBucket;
	}

	public void setDestBucket(String destBucket) {
		this.destBucket = destBucket;
	}
	
	public void setSourcePrefix(String sourcePrefix) {
		this.sourcePrefix = sourcePrefix;
		
	}
	public void setDestPrefix(String destPrefix) {
		this.destPrefix = destPrefix;
		
	}
	
	public void checkMd5()
	{
		this.md5check = true;
	}
	
	public boolean destinationmd5check()
	{
		try {
			StorageObject destObjectdetail = data.restService.getObjectDetails(this.destBucket, this.destinationKey);
			return destObjectdetail.getETag().equals(this.data.sourceObject.getETag());
		} catch (S3ServiceException e) {
////////////////////////////////////			System.out.println("Destination not found: " + data.sourceObjectKey + " EXCEPTION: " + e.getMessage());

		} catch (ServiceException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//System.out.println("FILE NOT CHECKED: " + data.sourceObjectKey + " EXCEPTION: " + e.getMessage());
		}
		return false;
	}

	public void run() {

		Map moveResult = null;
		Boolean skipflag = false;
		try {
			
			if(this.md5check)
			{
				skipflag = this.destinationmd5check();
			}
			if(!skipflag)
			{
				moveResult = data.restService.copyObject(this.sourceBucket, data.sourceObjectKey, this.destBucket, this.destinationObject, false);
				System.out.println("FILE MOVED: " + data.sourceObjectKey);
				System.out.println("to: " + this.destinationKey);
			} else {
				//System.out.println("MD5 check true - Do Not Copy: " + data.sourceObjectKey );
			}
		
		} catch (S3ServiceException e) {
			//System.out.println("Error: " + data.sourceObjectKey + " EXCEPTION: " + e.getMessage());

		} catch (ServiceException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//System.out.println("FILE NOT MOVED: " + data.sourceObjectKey + " EXCEPTION: " + e.getMessage());
		}

	}
	
	public void init(String delimiter) {
		this.destinationKey = this.data.sourceObject.getKey();
		if(this.sourcePrefix.length() > 0  && this.destPrefix.length() > 0)
		{
			this.destinationKey = this.destinationKey.replace(this.sourcePrefix, this.destPrefix);
		}
		if(this.sourcePrefix.length() == 0 && this.destPrefix.length() > 0)
		{
			this.destinationKey = this.destPrefix + delimiter + this.data.sourceObject.getKey();
		}
		this.destinationObject = (S3Object) data.sourceObject.clone();
		this.destinationObject.setKey(this.destinationKey);
	}

}