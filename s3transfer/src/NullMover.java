import org.jets3t.service.S3Service;
import org.jets3t.service.model.S3Object;


public class NullMover implements Runnable, Mover {
	
	private S3Object sourceObject;
	private String destBucket;
	private String sourceBucket;
	private String sourcePrefix;
	private String destPrefix;
	private String destinationKey;
	
	NullMover(final S3Service restService, final S3Object sourceObject, final String sourceObjectKey) {
		this.sourceObject = sourceObject;		
	}
	@Override
	public void run() {
		//System.out.println("Moving "+ sourceObject.getBucketName() + " -> "+ destBucket+ " : " + sourceObject.getKey());
		
	}
	@Override
	public void setSourceBucket(String sourceBucket) {
		this.sourceBucket = sourceBucket;
		
	}
	@Override
	public void setDestBucket(String destBucket) {
		this.destBucket = destBucket;
		
	}
	
	public void setSourcePrefix(String sourcePrefix) {
		this.sourcePrefix = sourcePrefix;
		
	}
	public void setDestPrefix(String destPrefix) {
		this.destPrefix = destPrefix;
		
	}
	
	public void checkMd5() {
		
	}
	
	public boolean destinationmd5check() {
		return false;
	}
	
	public void init(String delimiter) {
		this.destinationKey = this.sourceObject.getKey();
		System.out.println(this.destinationKey);
		if(this.sourcePrefix.length() > 0  && this.destPrefix.length() > 0)
		{
			this.destinationKey = this.destinationKey.replace(this.sourcePrefix, this.destPrefix);
		}
		if(this.sourcePrefix.length() == 0 && this.destPrefix.length() > 0)
		{
			this.destinationKey = this.destPrefix + delimiter + this.sourceObject.getKey();
		}
		System.out.println("becomes");
		System.out.println(this.destinationKey);
	}

}
