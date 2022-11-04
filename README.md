# ftp-to-s3-tool
This is a utility written in python to transfer files from an FTP server to an AWS S3 bucket. 

## Usage Notes

### AWS Client
The boto3 library is used to upload files to the specified S3 bucket. To run this you need to have the AWS cli installed and configured with a user that has access to the S3 bucket. 

### FTP Client
The FTP server is connected to using `ftplib` from the Python standard library. Right now there is no option to specify connection credentials for the FTP server. It will only work when run against a server that accepts anonymous connections.

### Directory vs Single File
This was origingally written to transfer all of the files from a given directory on the FTP server. There is also an option that would allow you to transfer a single file. By default the object prefixes will mirror the directory path on the FTP server. The output prefix is configurable.

### Parallel Execution
The transfer of all files from a given directory can be parallelized. The number of threads to run with can be configured. The biggest contributor to latency in the transfer process is IO, so multithreading can significantly improve the performance of the transfer of many files.

### Sample Script
Here is a sample of how the utility could be used as a python script:
```
    from ftp_to_s3 import FTPToS3
    import os
    import boto3

    ftp_host = os.environ["FTP_HOST"]
    s3_bucket_name = os.environ["S3_BUCKET"]
    num_threads = os.environ["NUM_THREADS"]
    log_level = os.environ["LOG_LEVEL"]
    num_threads = int(num_threads)

    ftp_to_s3 = FTPToS3(ftp_host, s3_bucket_name, num_threads=num_threads, log_level=log_level)

    # output is a list of failed filenames, the expected number transferred and # the actual number transferred
    failed_files, expected_num_files, actual_num_files = ftp_to_s3.transfer("/source/ftp/dir", "/target/s3/prefix")

    print(f"The expected number of files transferred: {expected_num_files}")
    print(f"The actual number of files transferred: {actual_num_files}")
```
