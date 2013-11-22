
public interface Mover {
	public void setSourceBucket(String sourceBucket);
	public void setDestBucket(String destBucket);
	public void setSourcePrefix(String sourcePrefix);
	public void setDestPrefix(String destPrefix);
	public void checkMd5();
	public void init(String delimiter);
	public boolean destinationmd5check();
}
