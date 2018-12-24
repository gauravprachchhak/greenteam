## Analysis of the effects of Greenhouse gases and Vehicles in Use on Global Temperature Changes from 2005 - 2014

**Note:**
* As mentioned in [Readme](https://csil-git1.cs.surrey.sfu.ca/greenteam/BigData_Project_Green_Team_2018/blob/master/README.md), you need [Microsoft Azure Account](https://azure.microsoft.com) to store the data in [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/), apply Machine Learning models with the help of [Azure ML Studio](https://docs.microsoft.com/en-us/azure/machine-learning/studio/) and to Visualize the data with [Microsoft Power BI](https://powerbi.microsoft.com/en-us/) with Azure Cosmos DB connector.

**Command to Run:**

* SSH to the cluster with this command

```python
ssh user@gateway.sfucloud.ca
```
* After that make sure you load the Apache Spark v 2.3.1

```python
module load spark
```
* After loading spark you can write below line to run the code 

```python 
time spark-submit --jars azure.jar project_weather.py BigDataProject/GHCN_Yearly/ BigDataProject/GHCN_Country/ BigDataProject/GHCN_Stations/  BigDataProject/CAIT_GHG_Emissions/ BigDataProject/Vehicle/ 
```

*Note: azure.jar is required because it connects spark to Azure Cosmos DB*

>We used azure-cosmosdb-spark connector and were successfully able to store the data on the cloud from Spark. Although there is one small problem, the spark code does successfully execute and stores the data on the cloud, but the code does not stop after that and just keeps on running. We used the code from Azure Cosmos DB Spark Connector documentation itself. For the same, we have raised a ticket in Microsoft Azure Help and Support have got a reply stating, “We will now begin working together to resolve your issue.” and are eagerly waiting for a solution to the problem.

**Important Links:**
* Main code file that does all the work is [project_weather.py](https://csil-git1.cs.surrey.sfu.ca/greenteam/BigData_Project_Green_Team_2018/blob/master/project_weather.py)
* The ML model that we created for this project can be [viewed here](https://gallery.cortanaintelligence.com/Experiment/Linear-Regression-to-predict-average-temperature-by-Country) and [here](https://gallery.cortanaintelligence.com/Experiment/Global-Temperature-Change)
* The analytics that we did on the final data can be [viewed here](https://app.powerbi.com/groups/me/dashboards/9a0155f9-0caa-4a7e-98d8-e80c1a4b1b5d)
* Azure-cosmosdb-spark connector (azure.jar) (For Spark 2.3) can be downloaded from [here](https://docs.databricks.com/spark/latest/data-sources/azure/cosmosdb-connector.html)

*Note: We were not able to publish our Database on Cosmos DB so you can not view it but some screenshots are there on our report. When you click on the azure links you can login using your Microsoft SFU credentials*
>*And if you are not able to login you can contact any of us at gprachch@sfu.ca, mgajjar@sfu.ca or vdhununj@sfu.ca*
