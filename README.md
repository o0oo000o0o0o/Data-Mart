# Data-Mart

## Summary

eBooks2go is a publishing company that offers a myriad of services in manuscript conversion, online distribution, and publication processes. Presently, data for these transactions are accumulated in various data storage systems. eBooks2go brought forth their challenges with their current data storage processes, and requested it's data team to help alleviate these pain points by creating a data warehouse of distributive sales data mart that aids in suggestive decision making. The goals of this project are the following:
To modify existing data into a structured format
To ensure that all future data remains structured
Combine data from various sources
Create a master table, combining customer data from various sources, using master data management techniques (https://mdmlist.com/2019/08/22/three-master-data-survivorship-approaches/ ​​)  
Create Key Performance Indicator (KPI) dashboards, using the newly cleaned/constructed data, to aid in business expansion
 
## DATA WAREHOUSE PLANNING

### Understanding the Data

The primary dataset belongs to eBooks2go home servers, eBooks2go.com and eBook2go.net. The secondary information is collected via KEAP (CRM platform). Service, distribution and conversion orders, distribution sales, and customer demographic information was collected via sources mentioned previously. This data was previously being stored in a MySQL database along with a grand scale of extra information collected for business purposes. Hence, querying for the purpose of marketing has become a hassle, which inspired the project into motion. For quality assessment of the data in hand, a source profiling document was created. (Document name). Using SQL queries this document was made to shine light on the descriptive nature of the attributes such as unique vs non-unique, null vs empty, pre-defined values, datatypes, and any missing values. 

KPIs that support business needs:
a.   Total Sales by Region
b.   Total Customer by country
c.    Customer churn
d.   Frequent Customer
e.   Distribution by Channel
f.     Sales by Channel
g.   Best Distribution Channels

### Dimensional Data Modelling
A star schema was created to obtain an intuitive and high performance data access. Fact and dimensional tables are generated as analytical measurables and their descriptive constraints respectively, by following data modelling best practices (https://docs.microsoft.com/en-us/power-query/dataflows/best-practices-for-dimensional-model-using-dataflows). The model is optimized by adding basic reference tables that support the descriptive dimensions.  A physical model is built in PostgreSQL using the work done so far.(see XXX for schema image). 

### Data Extraction and Transformation
 
With the source profiling document, a source to target mapping document was generated to outline the data mapping and transformation logic between landing and target sources. Python code is written to extract data from sources into a staging area where the transformation logic will be applied along with data cleansing and integration. Master data management system is performed manually using python code for customer data extracted from various sources. Python logic is written to solve issues like “dirty data”, missing values, duplicates etc. for the historical load. Same process was then followed in future for incremental data. 

### Master Data Management

Within the merged customer data, some cases exist where duplicate entries are recorded between the sources where the customer has few attributes recorded in one source, and updated/added afresh in another source. In this case, a golden record logic was written to transform and integrate customer data to generate an ideal record for that customer that stores consistent information. The logic was written on the basis of data quality dimensions like accuracy, completeness, relevancy, and uniformity per attribute of the record. 


### ETL Automation
By using Apache Airflow to schedule the automation of the ETL pipeline we were successfully able to move the historical data from staging to our data warehouse. The same cleaning process was used in our second pipeline which was to handle all incremental (future) data. 
