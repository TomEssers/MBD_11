# MBD_11

Data of flights from: https://zenodo.org/records/7923702
Citation: Matthias Schäfer, Martin Strohmeier, Vincent Lenders, Ivan Martinovic and Matthias Wilhelm. "Bringing Up OpenSky: A Large-scale ADS-B Sensor Network for Research".
In Proceedings of the 13th IEEE/ACM International Symposium on Information Processing in Sensor Networks (IPSN), pages 83-94, April 2014.

We have flight data from January 2019 till November 2022.

Data of flights from: https://ext.eurocontrol.int/
Citation: 

## Research

Research question: How did the lockdown periods during the COVID-19 period affect commercial flight traffic from the Netherlands to other European Countries in comparison to non-lockdown periods?

Additional Questions:
- Has the flight time between the different airports changed during these periods? (multiple statistics)
- How did the delay times evolve over the COVID period in comparison to the years previous to COVID? (multiple statistics)

COVID-19 period is defined as January 17th 2020 to 30th May 2022. This has been defined from using the timeline of the Dutch Rijksoverheid: https://www.rijksoverheid.nl/onderwerpen/coronavirus-tijdlijn

The lockdown period are defined as:
- Eerste lockdown: 15/03/2020 to 11/05/2020
- Gedeeltelijke lockdown: 13/10/2020 to 19/11/2020
- Tweede lockdown: 15/12/2020 to 28/04/2021
- Gedeeltelijke lockdown (na tweede lockdown): 28/04/2021 to 26/06/2021
- Derde lockdown (harde lockdown): 14/12/2021 to 26/01/2022

To only include passenger planes, we have created a list of all existing ICAO aircraft type designators. We used the Wikipedia list of all planes:  https://en.wikipedia.org/wiki/List_of_aircraft_type_designators 

For each of these planes, we looked up the maximum amount of passengers that the plane can take. Military planes, and cargo planes will have a maximum amount of 0 as we want to leave these out. For the sake of this research we will only use planes with at least 20 maximum passengers.

## Other

Data is stored on HDFS: /user/s2484765/project/...
