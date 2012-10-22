GovTrackGraph
=============

<<<<<<< HEAD
Neo4J Example of graphing GovTrack Data.
Additional information can be found at: http://www.intelliwareness.org/?p=230

Data
=============

The data is a compilation of data from www.govtrack.us. The data consists of congress members (Senate and House) that
sponsored or cosponsored bills from Congressional Sessions 107 through 112. Additionally, all of the bills for those sessions
where downloaded and parsed using the xmlparser.java file to create four files:

1. billsSubjects.dat - This lists all bills and the subjects associated with the bills. This is used to create unique nodes of 
the bill Subjects and link the bills to the subjects.

2. billsout.dat - This is a list of all bills introduced in congress.

3. billsCoSponsor.dat - This is a list of all congress members who have cosponsored bills.

4. billsActions.dat - This is a list of all actions that have happened with a bill.

Code
=============

parser/xmlParser.java - This parses all of the XML files that were downloaded from GovTrack.

Importer/Importer.java - This loads the data into Neo4J.

=======
Neo4J Example of graphing GovTrack Data
>>>>>>> ecddd70a78a9d2e7f45afe02ba542cd546d486b9
