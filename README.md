# **TX-BANK PROJECT**

---

## 📌 **Project Overview**

This project is an ETL pipeline for processing customer and account data using **Apache Spark**. The pipeline dynamically reads data from **local storage or AWS S3**, transforms it, and writes results back to **S3 or local storage**.

---

## 📌 **Setup & Installation**

### **1️⃣ Install Virtual Environment**

Create and activate a virtual environment:

```bash
# Install virtualenv
pip install virtualenv

# Create a new virtual environment
virtualenv env_name

# Activate virtual environment (Mac/Linux)
source env_name/bin/activate

# Activate virtual environment (Windows)
env_name\Scripts\activate
```

---

### **2️⃣ Install Required Libraries**

Install dependencies using pip:

```bash
pip install DBUtils pyspark delta-spark dotenv boto3
```

Or use a `requirements.txt` file:

```bash
pip install -r "requirements.txt"
```

---

### **3️⃣ Prepare JAR Files for S3 Support**

For Delta Lake and AWS S3 compatibility, download these JAR files:

```bash
mkdir -p jars  # Ensure target directory exists

wget -P jars https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar
wget -P jars https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar
wget -P jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar
wget -P jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar
```

---

### **4️⃣ Configure AWS Credentials**

AWS credentials must be set up correctly to access S3 storage.

**📌 Option 1: Using `.env` file (Local)**

Create `.env` in your project root:

```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_region
```

**📌 Option 2: Using Databricks Secrets**

Set secrets in Databricks Notebook:

```bash
databricks secrets put --scope aws-secrets --key AWS_ACCESS_KEY_ID
databricks secrets put --scope aws-secrets --key AWS_SECRET_ACCESS_KEY
databricks secrets put --scope aws-secrets --key AWS_REGION
```
