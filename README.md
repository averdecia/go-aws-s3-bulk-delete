# go-aws-s3-bulk-delete
The example is used to remove files from S3 by a given csv file

### The parameters are:
 - 0 -> File path for the input csv file
 - 1 -> S3 domain
 - 2 -> The amount of goroutines
 - 3 -> The pivot for printing progress on console
 - 4 -> The amount of file to remove from S3 on each request( 0 means one by one)
 - 5 -> File path for the output error list
 - 6 -> Prefix to be used on the S3 file Ids

# An example may be:
### ./main /tmp/withmax_3000-4000.csv s3.amazonaws.com 100 10 0 ./failed.csv prefix_
That will create 100 goroutines to remove from s3.amazonaws.com all the files on the withmax_3000-4000.csv file using "prefix_" as the file name prefix, printing the output each 10 files and saving the errors in failed.csv. 

The entry csv musta have the bucket name on the first column and the file id on the third column. An example may be:
#### **bucketfortest**, 72642345, **Filename.txt**, 2048B 
