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
to be fetched in order to limit data download to avoid downloading
the whole database at once bringing a huge data volume clogging
resources.
- The silver layer will read data from the bronze layer and make
some data treatments in order to clean it up unifying and removing
columns with same data and removing unwanted characters from columns
- The gold layer will read data from the silver layer and create
an aggregated view counting breweries by type, location and
type/location


## Run Locally  
### 1. Install Apache Airflow 
Please refer to the official documentation at:
<p><a href="https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html">https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html</a></p>

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
### 3. Import the new dag to Airflow
~~~bash
  $ airflow dags reserialize
~~~
### 4. Run Apache Airflow
~~~bash
  $ airflow standalone
~~~
Go to your web browser and access http://localhost:8080. If it
doesn't load, it's because Airflow's initialization process
is not finished yet. Reload the page until you get the login page.
![Login page image](https://github.com/lsrzj/breweries/blob/main/docs/login.png)

### 5. Login
You'll find the generated password into Airflow's directory, open it
~~~bash
  $ cat ~/airflow/simple_auth_manager_passwords.json.generated
~~~

### 6. Configure Airflow's Spark connection
Click on the Admin menu with a gear icon on the screen's right hand side
![Main page image](https://github.com/lsrzj/breweries/blob/main/docs/main.png)
Choose "Connections"
![Admin sub menu](https://github.com/lsrzj/breweries/blob/main/docs/admin_sub.png)
Click on "Add Connections" button on the right hand side of the screen
![Admin sub menu](https://github.com/lsrzj/breweries/blob/main/docs/admin_connections.png)
Add Spark's connection configurations as in the image below, scroll down to find the "Save" button to save the configurations:
![Admin sub menu](https://github.com/lsrzj/breweries/blob/main/docs/admin_add_connection.png)
The connection list should be like this now:
![Admin sub menu](https://github.com/lsrzj/breweries/blob/main/docs/admin_connections_list.png)


