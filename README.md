# Snoop-Technical-Assessment-20240202

Entrypoint - `src/main.py`  
Run using debugger to be able to use the `.env` file

Add new jobs/pipelines to the `src/job` directory  
Add shared scripts to `src/utilities`  
Add new database management scripts to `src/database`


## To do 
1. Unit Testing in PyTest
2. Add more error handling where it makes sense
3. Improve Pandas DataFrame schema enforcement in merge logic
4. Write documentation
5. Create some AWS resources using Terraform (S3 bucket for raw data, Networking, RDS Postgres instance, ECS task def's for running this code)
6. Store and retrieve database secrets from AWS Secrets Manager instead of hard coding
7. Dockerise this code
8. Create CICD PL to deploy code to AWS
9. Once all the infra is setup could start to think about integration testing