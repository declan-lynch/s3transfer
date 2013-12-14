s3transfer
==========

High performance Java application for Amazon S3 oeprations.

Operations supported:

1. copy from bucketA/prefixA to bucketX/prefixX, by default it will check the Etags and overwrite files that have changed
2. copy only if the key doesn't exist - doesn't overwrite files that exist (even if they are different), helps if your app overwrites stuff a lot
3. build structure - copies just folders so you can use it to fix s3fs problems

As S3 does not really have a directory structure, s3fs creates special files in every folder with the same name as the folder. Those files
have special attributes that make it possible for s3fs to figure out where it created a directory. The problem comes, when you want to use a s3 
bucket structure that was created without s3fs - it does not see the structure. A simple solution is to mkdir the folders there they already are in 
s3, but as you imagine, sometimes there is just a lot of folders to create.

Last time we run th s3fs fix task, It went though about 2,5 mln directories in 3 days, due to general s3fs slowness. I'm sure there is a way to actually just create the file itself without useing s3fs therefore fixing it from s3 side.

How to build, for those of you not familiar with Java:

- Install JDK 7
- Install Apache Maven 3
- run >mvn package
- run the app >java -jar [the jar from target/] and it should print out a nice CLI menu for you.


We are planning to do unit tests and add more commands to it, however if you are looking for a wider range of options please look at the excellent Amazon CLI Tools.

Please fork and have fun with it.
