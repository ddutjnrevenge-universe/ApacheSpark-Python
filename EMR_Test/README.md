# Running a Spark Job with AWS EMR

## Set Up EMR Cluster on AWS

1. **Create an EMR Cluster:**
   - Go to the AWS Management Console.
   - Navigate to the EMR service.
   - Click on "Create Cluster" and configure the cluster settings as needed.
   - Launch the cluster.

## Set Up Environment on Local Machine

### If Using Windows with WSL

1. **Move the Private Key File:**
   - Ensure your private key file `[ec2-key-pair-name].pem` is in the home directory.

2. **Open Terminal and Run the Following Commands:**
   ```bash
   wsl
   # Check if the private key file is in the home directory
   ls
   # Move the private key file to the home directory
   mv [ec2-key-pair-name].pem ~/ 
   # Change permissions of the private key file
   chmod 400 ~/[ec2-key-pair-name].pem 
   # Verify the permission changes
   ls -l ~/[ec2-key-pair-name].pem 
   ```
3. Update Security Group:

    Add an inbound rule to the EC2 instance's security group to allow SSH connections from `My IP` address or `Anywhere` (but not recommended for security reasons)
4. Connect to the EC2 Instance:
    ```bash
    ssh -i ~/[ec2-key-pair-name].pem hadoop@[ec2-public-dns]
    ```
    - Confirm the connection by typing "yes" and pressing enter.
    - If you see the "Amazon Linux 2023" screen with the bird and EMR logo, you are successfully connected.
    
### Prepare the Script for the Spark Job
1. Create and Edit the Spark Script:

``` bash
nano spark-etl.py
```   
1. Copy and Paste your Spark Job script (spark-etl.py)[] 
- Remember to `Overwrite` as by default, Spark will not overwrite an existing directory unless explicitly instructed to do so.
2. Run the Spark Job
Submit the Spark Job:

bash
Copy code
spark-submit spark-etl.py [s3 input URI path] [s3 output URI path]
If the job runs successfully, you will see the output in the terminal, including the number of records and the schema of the output.
The output will be saved in the specified S3 output folder in Parquet format.
Check Job Details:

View job details in the Spark UI through the Spark History Server and YARN Timeline Server.
Running Directly on the EMR Cluster
Add a Step to the EMR Cluster:
Go to the EMR console.
Click on the cluster you want to run the job on.
Click on the "Add step" button.
Configure the step with the following details:
Name: command-runner.jar
JAR Arguments:
bash
Copy code
spark-submit [s3 script URI path] [s3 input URI path] [s3 output URI path]
Upload the script to S3 and provide the S3 URI path for the script in the arguments.