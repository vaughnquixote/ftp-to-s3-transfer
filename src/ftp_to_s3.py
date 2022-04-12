from io import BytesIO
from ftplib import FTP
import boto3 
import concurrent.futures
import threading
import logging

class FTPToS3:

    def __init__(self, host, bucket, num_threads=1, log_level="info"):
        self.ftp_host = host
        self.s3_bucket = bucket
        self.num_threads = num_threads
        self.counter_lock = threading.Lock()
        self.file_count = 0
        self.init_logging(log_level)
        self.connect_to_s3()
    
    def connect_to_s3(self):
        try:
            self.s3_client = boto3.client("s3")
            self.logger.info("Established AWS S3 client")
        except:
            self.logger.error("Failed to establish AWS S3 client")

    def connect_to_ftp(self):
        try:
            self.ftp_connection = FTP(self.ftp_host)
            self.ftp_connection.login()
            self.logger.debug("Connected to FTP host")
        except Exception as e:
            self.logger.error("Failed to connect to FTP host")
    
    def init_logging(self, log_level):
        self.logger = logging.getLogger("ftp_to_s3")
        level = getattr(logging, log_level.upper(), 20)
        self.logger.setLevel(level)

        file_handler = logging.FileHandler("file_transfer.log")
        console_handler = logging.StreamHandler()
        file_handler.setLevel(level)
        console_handler.setLevel(level)

        formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)


    def transfer_files(self, ftp_directory_path, output_prefix=""):
        self.connect_to_ftp()
        self.file_count = 0
        self.ftp_connection.cwd(ftp_directory_path)
        
        # default to same directory structure as in the ftp server if
        # no output prefix is provided
        if output_prefix == "":
            self.output_prefix = ftp_directory_path
        else:
            self.output_prefix = output_prefix

        # list all of the files in the current directory
        filenames = self.ftp_connection.nlst()
        filenames = [f'{ftp_directory_path}/{f}'for f in filenames]

        # split the files across futures to be executed by the thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            results = executor.map(self.transfer_file, filenames)

        files_to_retry = [result for result in results if result != None]
        
        if len(files_to_retry) > 0:
            self.logger.info(f"{ftp_directory_path} -- retyring failed transfers")
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_threads) as retry_executor:
                retry_results = retry_executor.map(self.transfer_file, files_to_retry)
        else:
            self.logger.info("no failed transfers")
            retry_results = files_to_retry
        
        self.close()

        failed = [f for f in retry_results if f != None]
        return failed, len(filenames), self.file_count

    def transfer_file(self, ftp_filename, output_prefix=""):
        self.logger.debug(f"{ftp_filename} -- processing started")
        if output_prefix != "":
            self.output_prefix = output_prefix
        
        try:
            local_ftp_conn = FTP(self.ftp_host, timeout=60)
            self.logger.debug(f"{ftp_filename} -- thread local FTP connection established")
        except Exception as e:
            self.logger.error(f"{ftp_filename} -- failed to establish local FTP connection")
            self.logger.error(f"{ftp_filename} -- {str(e)}")
            return ftp_filename
        
        try:
            local_ftp_conn.login()
            self.logger.debug(f"{ftp_filename} -- successfully logged into FTP server")
        except Exception as e:
            self.logger.error(f"{ftp_filename} -- failed to log in to FTP server")
            self.logger.error(f"{ftp_filename} -- {str(e)}")
            self.close_local_ftp_conn(local_ftp_conn, ftp_filename)
            return ftp_filename

        ftp_command = "RETR " + ftp_filename
        curr_file_buffer = BytesIO()

        try:
            local_ftp_conn.retrbinary(ftp_command, curr_file_buffer.write)
            self.logger.info(f"{ftp_filename} -- read from server")
        except Exception as e:
            self.logger.error(f"{ftp_filename} -- failed to read file from server")
            self.logger.error(f"{ftp_filename} -- {str(e)}")
            self.close_local_ftp_conn(local_ftp_conn, ftp_filename)
            return ftp_filename

        curr_file_buffer.seek(0)
        
        try:
            filename = ftp_filename.split("/")[-1]
            self.s3_client.upload_fileobj(curr_file_buffer, self.s3_bucket, self.output_prefix + filename)
            self.logger.info(f"{ftp_filename} -- uploaded file to {self.output_prefix}")
        except Exception as e:
            self.logger.error(f"{ftp_filename} -- failed to upload file to {self.output_prefix}")
            self.logger.error(f"{ftp_filename} -- {str(e)}")
            self.close_local_ftp_conn(local_ftp_conn, ftp_filename)
            return ftp_filename
        

        self.close_local_ftp_conn(local_ftp_conn, ftp_filename)

        self.counter_lock.acquire()
        self.file_count += 1
        self.counter_lock.release()
        return None

    def close_local_ftp_conn(self, local_conn, ftp_filename):
        if local_conn != None:
            try:
                self.logger.debug(f"{ftp_filename} -- closing FTP connection")
                local_conn.quit()
                self.logger.debug(f"{ftp_filename} -- successfully closed local FTP connection")
            except Exception as e:
                self.logger.debug(f"{ftp_filename} -- failed to close FTP connection")
                self.logger.debug(f"{ftp_filename} -- {str(e)}")
                pass
    
    def close(self):
        if self.ftp_connection != None:
            try:
                self.ftp_connection.quit()
            except Exception as e:
                pass
        else:
            self.logger.debug("no FTP connection, nothing to close")