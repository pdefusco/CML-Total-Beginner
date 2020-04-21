## Predict Loan Applicant Default

#### The Project has 5 stages:

#### 1. Data Exploration in PySpark
Read data from S3, explore, and write modified data to Hive Spark table

#### 2. Upsampling in PySpark
Have a dry run with SMOTE on a smaller dataset to deal with Class Imbalance

#### 2A & 2B. Perform Upsampling with Experiments Feature
Submit multiple experiments in parallel, with the option of using hyperparameters

#### 3. Model Development
Create a Model Pipeline and save it to S3

#### 4. Batch Scoring
Schedule a Job to write predictions to a Hive Spark table

#### 5. Deploy Model
Deploy model to isolated Docker container and make predictions

#### 6. Application
Visualize predictions with Applications Feature
