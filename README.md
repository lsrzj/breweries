Breweries DB
============
This project is a medallion architecture data lake demo.
It downloads informations from the Open Brewery DB API, found at
https://openbrewerydb.org/.


## Features  
- Uses Apache Airflow as the orchestration platform
- The bronze layer is developed to download data from the API in
JSON format and store it as is. As the API works with pagination,
the user can choose the maximum number of pages to be fetched,
the page number to start at and the number of records per page
to be fetched. This will avoid downloading the whole database at
once bringing a huge data volume and, possibly, clogging resources.
- The silver layer will read data from the bronze layer and make
some data treatments in order to clean it up unifying and removing
columns with same data and removing unwanted characters from columns.
Then it'll save the output data partitioned by country/state in
Parquet format, a columnar storage format usually used in Big Data
processing to optimize data storage and retrival.
- The gold layer will read data from the silver layer and create
trhee aggregated views counting breweries by type, location and
type/location
- The output directory for processed data will be /path/to/airflow/data/[layer]/breweries

## Run Locally  
### 1. Install Apache Airflow 
Please refer to the official documentation at:
<p>
  <a href="https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html">
    https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html
  </a>
</p>
Apache's Airflow installation details and any other kind of configurations not given here
is out this documentation's scope. If you find problems installing and/or configuring
Apache Airflow, you can find informations on the internet and official documentation.


### 2. Clone the project  
~~~bash  
  $ git clone https://github.com/lsrzj/breweries.git
~~~


### 3. Put the project's scripts into Airflow's dags directory
Go to Airflow's dags directory. The default installation will,
usually, create the Airflow's directory structure into the user's
home directory:
~~~bash
  $ cd ~/airflow/dags
~~~
Create the breweries subdirectory:
~~~bash
  $ mkdir breweries
~~~
Go to breweries subdirectory you've created
~~~bash
  $ cd breweries
~~~
Copy the repository's contents into current directory
~~~bash
  $ cp -Rv /path/to/repo .
~~~
Copy .airflowignore file, not copied from the previous step into
current directory
~~~bash
  $ cp /path/to/repo/.airflowignore .
~~~


### 4. Import the new dag to Airflow
~~~bash
  $ airflow dags reserialize
~~~


### 5. Run Apache Airflow
~~~bash
  $ airflow standalone
~~~
Go to your web browser and access http://localhost:8080. If it
doesn't load, it's because Airflow's initialization process
is not finished yet. Reload the page until you get the login page.
![Login page image](https://github.com/lsrzj/breweries/blob/main/docs/login.png)

### 6. Login
You'll find the generated password for the **admin** user into Airflow's directory, open it
~~~bash
  $ cat ~/airflow/simple_auth_manager_passwords.json.generated
~~~

### 7. Configure Airflow's Spark connection
Click on the Admin menu with a gear icon on the screen's right hand side
![Main page image](https://github.com/lsrzj/breweries/blob/main/docs/main.png)
Choose "Connections"
![Admin sub menu](https://github.com/lsrzj/breweries/blob/main/docs/admin_sub.png)
Click on "Add Connections" button on the right hand side of the screen
![Admin Connections](https://github.com/lsrzj/breweries/blob/main/docs/admin_connections.png)
Add Spark's connection configurations as in the image below, scroll down to find the "Save" button to save the configurations:
![Admin add connection](https://github.com/lsrzj/breweries/blob/main/docs/admin_add_connection1.png)
![Admin add connection](https://github.com/lsrzj/breweries/blob/main/docs/admin_add_connection2.png)
The connection list should be like this now:
![Admin connections list](https://github.com/lsrzj/breweries/blob/main/docs/admin_connections_list.png)

### 8. Trigger brewery_pipeline DAG
Go to Dags page clicking on Dags menu on the left hand side menu, maybe you'll find many example DAGs there,
find the brewery_pipeline and click on it
![Dags list](https://github.com/lsrzj/breweries/blob/main/docs/dags.png)
Trigger the DAG clicking on the Trigger button on the right hand side of the screen.
![Dags list](https://github.com/lsrzj/breweries/blob/main/docs/brewery_pipeline.png)
