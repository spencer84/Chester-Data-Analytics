# Chester-Data-Analytics
Analysis of land data within Chester, England produced using available open source data

<h1> Purpose </h1>

<p1>
I am using this project as a training ground to learn about data science, data engineering, and software engineering by working with data local to the Chester area where I am currently based. The idea is to let this project gradually evolve in scope and build a website with various visualizations or applications to showcase different sub-projects.
</p1>

<h1> Data </h1>

<p1>
The success of this project is based on good quality, publicly available data. The data currently used in this project includes:
<ul>
<li><b>Energy Performance Certificates (EPC)</b> - Required for all dwellings, this provides granular detail on energy performance, floor space, and many other similar attributes</li>
<li> <b>HM Land Registry </b> - Transaction details for all home purchases, including price paid</li>
</ul>
These datasets are accessed via API and are programmatically updated on regular intervals. The EPC and Land Registry data can be combined by matching individual properties, though given the formatting differences, this requires a bit of extra work and is not perfect. However, with the records that can be merged, it is possible to use some of the EPC attributes to establish a relationship with the price paid. Currently, I am working to build a Flask web app to enable a simple house value estimate based off of a basic linear regression model.
</p1>
