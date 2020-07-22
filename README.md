# ApexSynchronizer
A client that acts as a go-between for PowerSchool and Apex Learning. [PowerSchool](https://www.powerschool.com/company/about-us/) is an Student Information System, a database management system with a focus on educational institutions. [Apex Learning](https://www.apexlearning.com/about-us) designs curricula for use in digital learning environments. This codebase facilitates the transfer of data between these two services, making use of their respective APIs. 

Apex Learning maintains a RESTful API that is managed via the [Apex Data Models](apex_synchronizer/apex_data_models) subpackage.

PowerSchool defines its own plugin system in which SQL queries are given in specialized XML files, which can be found in the [apex-plugin](apex-plugin) directory. Functional versions of this plugin can be found in the "Releases" section of this repository.

The code presumes that a few variables exist in the environment:

- CONSUMER_KEY: the Apex Learning consumer key, used in generating an access token for the Apex API
- SECRET_KEY: the Apex secret key, used in generating an access token for the Apex API
- PS_CLIENT_ID: the equivalent ID for PowerSchool
- PS_CLIENT_SECRET: the equivalent secret key for PowerSchool
- PS_URL: the domain URL for a school's PowerSchool server

