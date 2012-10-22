package importer;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.LuceneBatchInserterIndexProvider;

import java.io.*;
import java.util.*;

import org.neo4j.helpers.collection.MapUtil;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.FULLTEXT_CONFIG;

public class Importer {
    private static Report report;
    private BatchInserter db;
    private BatchInserterIndexProvider lucene;
 	
    public static final File STORE_DIR = new File("/Volumes/HD1/Users/dsfauth/congressBills");
    public static final File BILL_SUBJECTS = new File("/Volumes/HD1/Users/dsfauth/fecBills/data/billsSubjects.dat");
    public static final File BILL_CONGRESS = new File("/Volumes/HD1/Users/dsfauth/fecBills/data/activeCongress.dta");
    public static final File BILL_DETAILS = new File("/Volumes/HD1/Users/dsfauth/fecBills/data/billsout.dat");
    public static final File BILL_ALLSUBJECTS = new File("/Volumes/HD1/Users/dsfauth/fecBills/data/billsSubjects.dat");
    public static final File BILL_COSPONSORS = new File("/Volumes/HD1/Users/dsfauth/fecBills/data/billsCoSponsor.dat");
    public static final File BILL_ACTIONS = new File("/Volumes/HD1/Users/dsfauth/fecBills/data/billsActions.dat");
    public static final int USERS = 3000000;
    
    enum MyRelationshipTypes implements RelationshipType {SUPPORTS, SPONSORS, COSPONSORS, REFLECTS, ACTIVITY, FOR, CONTRIBUTES, RECEIVES, GAVE,SUPERPACGIFT,SUPERPACEXPEND,SUPERPACACTION}
   	Map<String,Long> cache = new HashMap<String,Long>(USERS);
    Map<String,Long> contribCache = new HashMap<String,Long>(USERS);
    
    public Importer(File graphDb) {
    	Map<String, String> config = new HashMap<String, String>();
    	try {
	        if (new File("batch.properties").exists()) {
	        	System.out.println("Using Existing Configuration File");
	        } else {
		        System.out.println("Writing Configuration File to batch.properties");
				FileWriter fw = new FileWriter( "batch.properties" );
                fw.append( "use_memory_mapped_buffers=true\n"
                        + "neostore.nodestore.db.mapped_memory=100M\n"
                        + "neostore.relationshipstore.db.mapped_memory=500M\n"
                        + "neostore.propertystore.db.mapped_memory=1G\n"
                        + "neostore.propertystore.db.strings.mapped_memory=200M\n"
		                 + "neostore.propertystore.db.arrays.mapped_memory=0M\n"
		                 + "neostore.propertystore.db.index.keys.mapped_memory=15M\n"
		                 + "neostore.propertystore.db.index.mapped_memory=15M" );
		        fw.close();
	        }
	        
        config = MapUtil.load( new File(
                "batch.properties" ) );

        } catch (Exception e) {
    		System.out.println(e.getMessage());
        }
                
        db = createBatchInserter(graphDb, config);
        lucene = createIndexProvider();
        report = createReport();
    }

    protected StdOutReport createReport() {
        return new StdOutReport(10 * 1000 * 1000, 100);
    }

    protected LuceneBatchInserterIndexProvider createIndexProvider() {
        return new LuceneBatchInserterIndexProvider(db);
    }

    protected BatchInserter createBatchInserter(File graphDb, Map<String, String> config) {
        return BatchInserters.inserter(graphDb.getAbsolutePath(), config);
    }

    public static void main(String[] args) throws IOException {
    	   
 //       if (args.length < 3) {
  //          System.err.println("Usage java -jar batchimport.jar data/dir nodes.csv relationships.csv [node_index node-index-name fulltext|exact nodes_index.csv rel_index rel-index-name fulltext|exact rels_index.csv ....]");
   //     }
//        File graphDb = new File(args[0]);
        File graphDb = STORE_DIR;
        File subjectsFile = BILL_SUBJECTS;
        File congressFile = BILL_CONGRESS;
        File billDetailsFile = BILL_DETAILS;
        File billAllSubjects = BILL_ALLSUBJECTS;
        File billCoSponsors = BILL_COSPONSORS;
        File billFileActions = BILL_ACTIONS;
//        File nodesFile = new File(args[1]);
//        File relationshipsFile = new File(args[2]);
        File indexFile;
        String indexName;
        String indexType;
        
        if (graphDb.exists()) {
            FileUtils.deleteRecursively(graphDb);
        }
 
        Importer importBatch = new Importer(graphDb);
        try {
            if (subjectsFile.exists()) importBatch.importSubjects(new FileReader(subjectsFile));
            if (congressFile.exists()) importBatch.importCongress(new FileReader(congressFile));
            if (billDetailsFile.exists()) importBatch.importBills(new FileReader(billDetailsFile));
            if (billAllSubjects.exists()) importBatch.importBillSubjects(new FileReader(billAllSubjects));
            if (billCoSponsors.exists()) importBatch.importBillCoSponsors(new FileReader(billCoSponsors));
            if (billFileActions.exists()) importBatch.importBillActions(new FileReader(billFileActions));
 
            //           if (relationshipsFile.exists()) importBatch.importRelationships(new FileReader(relationshipsFile));
//			for (int i = 3; i < args.length; i = i + 4) {
//				indexFile = new File(args[i + 3]);
 //               if (!indexFile.exists()) continue;
  //              indexName = args[i+1];
   //             indexType = args[i+2];
    //            BatchInserterIndex index = args[i].equals("node_index") ? importBatch.nodeIndexFor(indexName, indexType) : importBatch.relationshipIndexFor(indexName, indexType);
     //           importBatch.importIndex(indexName, index, new FileReader(indexFile));
	//		}
            System.out.println("finished");
		} finally {
            importBatch.finish();
        }
    }

    void finish() {
        lucene.shutdown();
        db.shutdown();
 //       report.finish();
    }

    public static class Data {
        private Object[] data;
        private final int offset;
        private final String delim;
        private final String[] fields;
        private final String[] lineData;
        private final Type types[];
        private final int lineSize;
        private int dataSize;

        public Data(String header, String delim, int offset) {
            this.offset = offset;
            this.delim = delim;
            fields = header.split(delim);
            lineSize = fields.length;
            types = parseTypes(fields);
            lineData = new String[lineSize];
            createMapData(lineSize, offset);
        }

        private Object[] createMapData(int lineSize, int offset) {
            dataSize = lineSize - offset;
            data = new Object[dataSize*2];
            for (int i = 0; i < dataSize; i++) {
                data[i * 2] = fields[i + offset];
            }
            return data;
        }

        private Type[] parseTypes(String[] fields) {
            Type[] types = new Type[lineSize];
            Arrays.fill(types, Type.STRING);
            for (int i = 0; i < lineSize; i++) {
                String field = fields[i];
                int idx = field.indexOf(':');
                if (idx!=-1) {
                   fields[i]=field.substring(0,idx);
                   types[i]= Type.fromString(field.substring(idx + 1));
                }
            }
            return types;
        }

        private int split(String line) {
            final StringTokenizer st = new StringTokenizer(line, delim,true);
//            System.out.println(line);
            int count=0;
            for (int i = 0; i < lineSize; i++) {
                String value = st.nextToken();
                if (value.equals(delim)) {
                    lineData[i] = null;
                } else {
                    lineData[i] = value.trim().isEmpty() ? null : value;
                    if (i< lineSize -1) st.nextToken();
                }
                if (i >= offset && lineData[i]!=null) {
                    data[count++]=fields[i];
                    data[count++]=types[i].convert(lineData[i]);
                }
            }
            return count;
        }

        public Map<String,Object> update(String line, Object... header) {
            int nonNullCount = split(line);
            if (header.length > 0) {
                System.arraycopy(lineData, 0, header, 0, header.length);
            }

            if (nonNullCount == dataSize*2) {
                return map(data);
            }
            Object[] newData=new Object[nonNullCount];
            System.arraycopy(data,0,newData,0,nonNullCount);
            return map(newData);
        }

    }

    static class StdOutReport implements Report {
        private final long batch;
        private final long dots;
        private long count;
        private long total = System.currentTimeMillis(), time, batchTime;

        public StdOutReport(long batch, int dots) {
            this.batch = batch;
            this.dots = batch / dots;
        }

        @Override
        public void reset() {
            count = 0;
            batchTime = time = System.currentTimeMillis();
        }

        @Override
        public void finish() {
            System.out.println("\nTotal import time: "+ (System.currentTimeMillis() - total) / 1000 + " seconds ");
        }

        @Override
        public void dots() {
            if ((++count % dots) != 0) return;
            System.out.print(".");
            if ((count % batch) != 0) return;
            long now = System.currentTimeMillis();
            System.out.println(" "+ (now - batchTime) + " ms for "+batch);
            batchTime = now;
        }

        @Override
        public void finishImport(String type) {
            System.out.println("\nImporting " + count + " " + type + " took " + (System.currentTimeMillis() - time) / 1000 + " seconds ");
        }
    }

    void importIndiv(Reader reader, int flag) throws IOException {
        String[] strTemp;
        BufferedReader bf = new BufferedReader(reader);
        final Data data = new Data(bf.readLine(), "\\|", 0);
        String line;
        report.reset();
        	LuceneBatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(db); 	
        	BatchInserterIndex idxIndivContrib = indexProvider.nodeIndex( "individuals", MapUtil.stringMap( "type", "exact" ) );
        	idxIndivContrib.setCacheCapacity( "indivName", 2000000 );
        while ((line = bf.readLine()) != null) {
        	strTemp = line.split("\\|");
        	long caller = db.createNode(data.update(line));
        	//System.out.println(caller);
        	Map<String, Object> properties = MapUtil.map( "indivName", strTemp[1]);
    		properties.put("indivCity", strTemp[2]);
    		properties.put("indivState", strTemp[3]);
    		properties.put("indivZip", strTemp[4]);
    		properties.put("indivEmp", strTemp[5]);
    		properties.put("indivOCC", strTemp[6]);
    		idxIndivContrib.add(caller,properties);
        	cache.put(strTemp[0], caller);
           
            report.dots();
        }
        idxIndivContrib.flush();
        indexProvider.shutdown();
        report.finishImport("Nodes");
    }
    
    void importCommittees(Reader reader) throws IOException {
        String[] strTemp;
        BufferedReader bf = new BufferedReader(reader);
        final Data data = new Data(bf.readLine(), "\\|", 0);
        String line;
        report.reset();
        LuceneBatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(db); 	
        BatchInserterIndex idxCommittees = indexProvider.nodeIndex( "committees", MapUtil.stringMap( "type", "exact" ) );
        idxCommittees.setCacheCapacity( "commName", 100000 );

        while ((line = bf.readLine()) != null) {
        	strTemp = line.split("\\|");
        	long committee = db.createNode(data.update(line));
        	Map<String, Object> properties = MapUtil.map( "commName", strTemp[1]);
    		properties.put("commID", strTemp[0]);
    		properties.put("commTreas", strTemp[3]);
    		properties.put("commState", strTemp[7]);
    		idxCommittees.add(committee,properties);
        	//System.out.println(caller);
        	cache.put(strTemp[0], committee);
           idxCommittees.flush();
            report.dots();
        }
        idxCommittees.flush();
        indexProvider.shutdown();
        
        report.finishImport("Nodes");
    }

    void importSubjects(Reader reader) throws IOException {
        String[] strTemp;
        BufferedReader bf = new BufferedReader(reader);
        final Data data = new Data(bf.readLine(), "\\|", 0);
        String line;
        report.reset();
        LuceneBatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(db); 	
        BatchInserterIndex idxSubjects = indexProvider.nodeIndex( "subjects", MapUtil.stringMap( "type", "exact" ) );
        idxSubjects.setCacheCapacity( "subject", 100000 );

        while ((line = bf.readLine()) != null) {
        	strTemp = line.split("\\|");
        	Long lCommId = cache.get(strTemp[1]);
            if (lCommId!=null){
            	
            }else{
            	long lSubject = db.createNode(data.update(line));
            	Map<String, Object> properties = MapUtil.map( "subject", strTemp[1]);
            	idxSubjects.add(lSubject,properties);
            	//System.out.println(caller);
            	cache.put(strTemp[1], lSubject);
            	idxSubjects.flush();
            	report.dots();
            }
            
        }
        idxSubjects.flush();
        indexProvider.shutdown();
        
        report.finishImport("Subjects");
    }

    void importCongress(Reader reader) throws IOException {
        String[] strTemp;
        BufferedReader bf = new BufferedReader(reader);
        final Data data = new Data(bf.readLine(), "\\|", 0);
        String line;
        report.reset();
        LuceneBatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(db); 	
        BatchInserterIndex idxCongress = indexProvider.nodeIndex( "congress", MapUtil.stringMap( "type", "exact" ) );
        idxCongress.setCacheCapacity( "congressID", 100000 );

        while ((line = bf.readLine()) != null) {
        	strTemp = line.split("\\|");
//        	System.out.println(line);
        	long lCongress = db.createNode(data.update(line));
        	Map<String, Object> properties = MapUtil.map( "congressID", strTemp[0]);
    		properties.put("lastName", strTemp[1]);
    		properties.put("firstName", strTemp[2]);
    		properties.put("title", strTemp[17]);
    		properties.put("State", strTemp[18]);
    		properties.put("Party", strTemp[26]);
         	idxCongress.add(lCongress,properties);
        	//System.out.println(caller);
        	cache.put(strTemp[0], lCongress);
        	idxCongress.flush();
            report.dots();
        }
        idxCongress.flush();
        indexProvider.shutdown();
        
        report.finishImport("Nodes");
    }

    
    void importBills(Reader reader) throws IOException {
   	 String[] strTemp;
       BufferedReader bf = new BufferedReader(reader);
       final Data data = new Data(bf.readLine(), "\\|", 0);
       String line;
       LuceneBatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(db); 	
       BatchInserterIndex idxSuperPacExpend = indexProvider.nodeIndex( "bills", MapUtil.stringMap( "type", "exact" ) );
       idxSuperPacExpend.setCacheCapacity( "BillID", 200000 );

       report.reset();
       while ((line = bf.readLine()) != null) {
       	strTemp = line.split("\\|");
 //      	System.out.println(line);
       	long lBillId = db.createNode(data.update(line));
       	Long lSponsorId = cache.get(strTemp[6]);
           if (lSponsorId!=null){
           	db.createRelationship(lSponsorId, lBillId, MyRelationshipTypes.SPONSORS, null);
           }         
           
           Map<String, Object> properties = MapUtil.map( "BillID", strTemp[0]);
   		properties.put("Session", strTemp[2]);
        properties.put("Title", strTemp[5]);
   		properties.put("Summary", strTemp[7]);
   		idxSuperPacExpend.add(lBillId,properties);
   		cache.put(strTemp[0], lBillId);
           report.dots();
       }
       idxSuperPacExpend.flush();
       indexProvider.shutdown();
       System.out.println("Finished with Bills");
       report.finishImport("Bills");
   }
    
    void importBillSubjects(Reader reader) throws IOException {
        String[] strTemp;
        BufferedReader bf = new BufferedReader(reader);
        final Data data = new Data(bf.readLine(), "\\|", 0);
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {
        	strTemp = line.split("\\|");
        	Long lBillID = cache.get(strTemp[0]);
          	Long lSubjectID = cache.get(strTemp[1]);
            if (lBillID!=null){
               	db.createRelationship(lBillID, lSubjectID, MyRelationshipTypes.REFLECTS, null);
           }
        	//System.out.println(caller);
           
            report.dots();
        }
        report.finishImport("Bills to Subjects");
    }
    
    void importBillActions(Reader reader) throws IOException {
      	 String[] strTemp;
         BufferedReader bf = new BufferedReader(reader);
         final Data data = new Data(bf.readLine(), "\\|", 0);
         String line;
         LuceneBatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(db); 	
         BatchInserterIndex idxBillAction = indexProvider.nodeIndex( "Actions", MapUtil.stringMap( "type", "fulltext" ) );
         idxBillAction.setCacheCapacity( "Action", 200000 );

         report.reset();
         while ((line = bf.readLine()) != null) {
         	strTemp = line.split("\\|");
         //	System.out.println(line);
         	long lActionID = db.createNode(data.update(line));
         	Long lBillID = cache.get(strTemp[0]);
             if (lBillID!=null){
             	db.createRelationship(lActionID, lBillID, MyRelationshipTypes.ACTIVITY, null);
             }         
             
            Map<String, Object> properties = MapUtil.map( "Description", strTemp[3]);
     		idxBillAction.add(lActionID,properties);
     	    report.dots();
         }
         idxBillAction.flush();
         indexProvider.shutdown();
         System.out.println("Finished with Bill Actions");
         report.finishImport("Bill Actions");
       }

    
    void importBillCoSponsors(Reader reader) throws IOException {
        String[] strTemp;
        BufferedReader bf = new BufferedReader(reader);
        final Data data = new Data(bf.readLine(), "\\|", 0);
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {
        	strTemp = line.split("\\|");
        	if (strTemp[0].equals("112s1720")){
        		System.out.println(strTemp[1]);
        	}
        	Long lBillID = cache.get(strTemp[0]);
          	Long lCoSponsor = cache.get(strTemp[1]);
            if (lBillID!=null && lCoSponsor!=null){
               	db.createRelationship(lCoSponsor,lBillID, MyRelationshipTypes.COSPONSORS, null);
           } else {
        	   System.out.println("Cosponsor id is: " + strTemp[1]);
           }
        	//System.out.println(caller);
           
            report.dots();
        }
        report.finishImport("Bills to CoSponsors");
    }
    
    void importSuperPac(Reader reader) throws IOException {
        String[] strTemp;
        BufferedReader bf = new BufferedReader(reader);
        final Data data = new Data(bf.readLine(), "\\|", 0);
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {
        	strTemp = line.split("\\|");
        	Long lCommId = cache.get(strTemp[1]);
            if (lCommId!=null){
            	
            }else{
            	long caller = db.createNode(data.update(line));
            	cache.put(strTemp[0], caller);
            }
        	//System.out.println(caller);
           
            report.dots();
        }
        report.finishImport("Nodes");
    }

    void importSuperPacContrib(Reader reader) throws IOException {
    	String[] strTemp;
        BufferedReader bf = new BufferedReader(reader);
        final Data data = new Data(bf.readLine(), "\\|", 0);
        String line;
        LuceneBatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(db); 	
        BatchInserterIndex idxSuperPacContribs = indexProvider.nodeIndex( "superPacDonations", MapUtil.stringMap( "type", "fulltext" ) );
        idxSuperPacContribs.setCacheCapacity( "commID", 200000 );

        report.reset();
        while ((line = bf.readLine()) != null) {
        	strTemp = line.split("\\|");
        	long pacCont = db.createNode(data.update(line));
        	Long lCommId = cache.get(strTemp[2]);
            if (lCommId!=null){
            	db.createRelationship(lCommId, pacCont, MyRelationshipTypes.SUPERPACGIFT, null);
            }   
            
            Map<String, Object> properties = MapUtil.map( "commID", strTemp[2]);
    		properties.put("donatingOrg", strTemp[3]);
    		properties.put("donorLast", strTemp[4]);
    		properties.put("donorState", strTemp[7]);
    		properties.put("donorFullName", strTemp[15]);
    		idxSuperPacContribs.add(pacCont,properties);
            report.dots();
        }
        System.out.println("Finished with SUPERPAC Contributions");
        report.finishImport("Nodes");
        idxSuperPacContribs.flush();
        indexProvider.shutdown();
    }
    
    void importSuperPacExpend(Reader reader) throws IOException {
    	 String[] strTemp;
        BufferedReader bf = new BufferedReader(reader);
        final Data data = new Data(bf.readLine(), "\\|", 0);
        String line;
        LuceneBatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(db); 	
        BatchInserterIndex idxSuperPacExpend = indexProvider.nodeIndex( "superPacExpend", MapUtil.stringMap( "type", "exact" ) );
        idxSuperPacExpend.setCacheCapacity( "commID", 200000 );

        report.reset();
        while ((line = bf.readLine()) != null) {
        	strTemp = line.split("\\|");
        //	System.out.println(line);
        	long pacExpend = db.createNode(data.update(line));
        	Long lCommId = cache.get(strTemp[2]);
        	Long lCandId = cache.get(strTemp[7]);
            if (lCommId!=null){
            	db.createRelationship(lCommId, pacExpend, MyRelationshipTypes.SUPERPACEXPEND, null);
            }         
            if (lCandId!=null){
            	db.createRelationship(lCandId, pacExpend, MyRelationshipTypes.SUPERPACACTION, null);
            }  
            
            Map<String, Object> properties = MapUtil.map( "commID", strTemp[2]);
    		properties.put("isSuperPAC", strTemp[3]);
    		properties.put("candidate", strTemp[5]);
    		properties.put("SUPPORT_OPPOSE", strTemp[6]);
    		properties.put("expendAmt", strTemp[12]);
    		idxSuperPacExpend.add(pacExpend,properties);
            report.dots();
        }
        idxSuperPacExpend.flush();
        indexProvider.shutdown();
        System.out.println("Finished with SUPERPAC Expenditures");
        report.finishImport("Nodes");
    }

    void importCandidates(Reader reader) throws IOException {
        String[] strTemp;
        BufferedReader bf = new BufferedReader(reader);
        final Data data = new Data(bf.readLine(), "\\|", 0);
        String line;
        report.reset();
        LuceneBatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(db);
    	
        BatchInserterIndex candidates = indexProvider.nodeIndex( "candidates", MapUtil.stringMap( "type", "exact" ) );
        candidates.setCacheCapacity( "candidateName", 100000 );
        while ((line = bf.readLine()) != null) {
        	strTemp = line.split("\\|");
        	long polCand = db.createNode(data.update(line));
        		Map<String, Object> properties = MapUtil.map( "candidateName", strTemp[1]);
        		properties.put("candidateID", strTemp[0]);
        		properties.put("candidateParty", strTemp[3]);
        		properties.put("candidateOfficeState", strTemp[5]);
        		properties.put("candidateElectionYear",strTemp[4]);
        		candidates.add(polCand,properties);
        		candidates.flush();
        	Long lCommId = cache.get(strTemp[10]);
            if (lCommId!=null){
            	db.createRelationship(lCommId, polCand, MyRelationshipTypes.SUPPORTS, null);
            }         
            report.dots();
        }
    	candidates.flush();
    	indexProvider.shutdown();
        report.finishImport("Nodes");
    }
    
    void importContrib(Reader reader) throws IOException {
        String[] strTemp;
        BufferedReader bf = new BufferedReader(reader);
        final Data data = new Data(bf.readLine(), "\\|", 0);
        String line;
        report.reset();
        LuceneBatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(db);
    	BatchInserterIndex contributors = indexProvider.nodeIndex( "contributions", MapUtil.stringMap( "type", "exact" ) );
        contributors.setCacheCapacity( "commID", 2500000 );
       
        while ((line = bf.readLine()) != null) {
        	strTemp = line.split("\\|",-1);
        	//System.out.println(line);
        	long indContr = db.createNode(data.update(line));
        	Long lCommId = cache.get(strTemp[1]);
        	Long lIndivId = cache.get(strTemp[0]);
            if (lCommId!=null){
            	db.createRelationship(lCommId, indContr, MyRelationshipTypes.RECEIVES, null);
            	
            }  
            if (lIndivId!=null){
            	long indRel = db.createRelationship(lIndivId, indContr, MyRelationshipTypes.GAVE, null);
            }   
            
            try{
        		Map<String, Object> properties = MapUtil.map( "commID", strTemp[1]);
        		properties.put("contribDate", strTemp[3]);
        		properties.put("contribAmt", strTemp[4]);
        		contributors.add(indContr,properties);
        	} catch (Exception e){
        		System.out.println(e);
        	}
            report.dots();
            
        }
        contributors.flush();
        indexProvider.shutdown();
        report.finishImport("Nodes");
    }
   
    
    void importRelationships(Reader reader) throws IOException {
        BufferedReader bf = new BufferedReader(reader);
        final Data data = new Data(bf.readLine(), "\\|", 3);
        Object[] rel = new Object[3];
        final RelType relType = new RelType();
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {
            final Map<String, Object> properties = data.update(line, rel);
            db.createRelationship(id(rel[0]), id(rel[1]), relType.update(rel[2]), properties);
            report.dots();
        }
        report.finishImport("Relationships");
    }

    void importIndex(String indexName, BatchInserterIndex index, Reader reader) throws IOException {

        BufferedReader bf = new BufferedReader(reader);
        
        final Data data = new Data(bf.readLine(), "\\|", 1);
        Object[] node = new Object[1];
        String line;
        report.reset();
        while ((line = bf.readLine()) != null) {        
            final Map<String, Object> properties = data.update(line, node);
            index.add(id(node[0]), properties);
            report.dots();
        }
                
        report.finishImport("Done inserting into " + indexName + " Index");
    }

    private BatchInserterIndex nodeIndexFor(String indexName, String indexType) {
        return lucene.nodeIndex(indexName, configFor(indexType));
    }

    private BatchInserterIndex relationshipIndexFor(String indexName, String indexType) {
        return lucene.relationshipIndex(indexName, configFor(indexType));
    }

    private Map<String, String> configFor(String indexType) {
        return indexType.equals("fulltext") ? FULLTEXT_CONFIG : EXACT_CONFIG;
    }

    static class RelType implements RelationshipType {
        String name;

        public RelType update(Object value) {
            this.name = value.toString();
            return this;
        }

        public String name() {
            return name;
        }
    }

    public enum Type {
        BOOLEAN {
            @Override
            public Object convert(String value) {
                return Boolean.valueOf(value);
            }
        },
        INT {
            @Override
            public Object convert(String value) {
                return Integer.valueOf(value);
            }
        },
        LONG {
            @Override
            public Object convert(String value) {
                return Long.valueOf(value);
            }
        },
        DOUBLE {
            @Override
            public Object convert(String value) {
                return Double.valueOf(value);
            }
        },
        FLOAT {
            @Override
            public Object convert(String value) {
                return Float.valueOf(value);
            }
        },
        BYTE {
            @Override
            public Object convert(String value) {
                return Byte.valueOf(value);
            }
        },
        SHORT {
            @Override
            public Object convert(String value) {
                return Short.valueOf(value);
            }
        },
        CHAR {
            @Override
            public Object convert(String value) {
                return value.charAt(0);
            }
        },
        STRING {
            @Override
            public Object convert(String value) {
                return value;
            }
        };

        private static Type fromString(String typeString) {
            if (typeString==null || typeString.isEmpty()) return Type.STRING;
            try {
                return valueOf(typeString.toUpperCase());
            } catch (Exception e) {
                throw new IllegalArgumentException("Unknown Type "+typeString);
            }
        }

        public abstract Object convert(String value);
    }

    private long id(Object id) {
        return Long.parseLong(id.toString());
    }
}