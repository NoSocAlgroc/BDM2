# BDM Course Project: P2

This repository contains our solution for the P2 of the BDM course project and may be found:

https://github.com/NoSocAlgroc/BDM2

It contains the following:

- Formatted zone scripts for each source: These load the local files and perform the row completion using the lookups. After that, they are uploaded to HDFS on the VM.
- Exploitation zone scripts: These load the stored files from HDFS and upload them to postgres also on the VM
- Exploitation zone SQL: This is the SQL file used to generate the views of the exploitation zone. No need to run as the database is hosted on the VM.
- Model scripts: This is the Jupyter Notebook containing the model. It reads the corresponding view of the exploitation zone and trains and evaluates a Random Forest Classifier to predict the price of an idealista listing given its properties and the data from the district, obtained from the other sources.
- Tableau project: Project that contains our analytical dashboards.


All intermediate files are already generated, so any script may be ran independently of the others.

To run them all:

    python formatted_idealista.py

    python formatted_income.py

    python formatted_prices.py

    python exploitation.py

Exploitation script appends rows, so be wary of duplicate data if it is run multiple times. Tables may be cleared with:

    delete from idealista;
    delete from income;
    delete from price;

To view the dashboards, open BDM-2-Dashboards.twb with Tableau.

To train and test the model, run model.ipynb notebook.
