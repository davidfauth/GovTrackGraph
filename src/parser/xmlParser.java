package parser;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import java.io.*;
import java.util.*;
 

/**
 * This sample program using fully qualified names while
 * accessing the contents of DOM-document.
 */

public class xmlParser {

	public static void listFilesAndFilesSubDirectories(String directoryName){
	      File directory = new File(directoryName);
 
        //get all the files from a directory
        File[] fList = directory.listFiles();
 
        for (File file : fList){
            if (file.isFile()){
   //               System.out.println(file.getAbsolutePath());
            } else if (file.isDirectory()){
                listFilesAndFilesSubDirectories(file.getAbsolutePath());
            }
        }
    }
	
  public static void main(String[] args) {
    try {
      // load the document from a file:
      DocumentBuilderFactory factory = 
        DocumentBuilderFactory.newInstance();
      factory.setValidating(false);
      
      String strBillDetails = "";
      String strBillCoSponsor = "";
      String strBillNumber = "";
      String strBillSubjects = "";
      String strSummary = "";
      String strAction = "";
      String strActionDate = "";
      String strActionState = "";
      String strTmpDir = "";

      BufferedWriter output=null;
      File file=new File("/Volumes/HD1/Users/dsfauth/fecdata/billsout.dat");
      output = new BufferedWriter(new FileWriter(file));
      output.write("BillID|BillNumber|Session|Type|Introduced|Title|Sponsor|Summary");
      output.newLine();
      BufferedWriter output2=null;
      File file2=new File("/Volumes/HD1/Users/dsfauth/fecdata/billsCoSponsor.dat");
      output2 = new BufferedWriter(new FileWriter(file2));
      BufferedWriter output3=null;
      File file3=new File("/Volumes/HD1/Users/dsfauth/fecdata/billsSubjects.dat");
      output3 = new BufferedWriter(new FileWriter(file3));
      output3.write("Bill|Topic");
      output3.newLine();
      BufferedWriter output4=null;
      File file4=new File("/Volumes/HD1/Users/dsfauth/fecdata/billsActions.dat");
      output4 = new BufferedWriter(new FileWriter(file4));
      output4.write("BillID|Action|State|Activity");
      output4.newLine();
      
      listFilesAndFilesSubDirectories("/Volumes/HD1/Users/dsfauth/GovTrack/111");

//      }  

      for (int counter = 107; counter<113; counter++){
    	  strTmpDir =   "/Volumes/HD1/Users/dsfauth/GovTrack/" + counter + "/bills";
      
      File dir = new File(strTmpDir);
      	System.out.println(strTmpDir);
       for (File child : dir.listFiles()){
    	  
      DocumentBuilder loader = factory.newDocumentBuilder();
//      Document document = loader.parse("/Volumes/HD1/Users/dsfauth/fecdata/h5856.xml");
      Document document = loader.parse(child);

      NodeList nodeList = document.getElementsByTagName("bill");
	  for (int s = 0; s < 1; s++) {
	    Node fstNode = nodeList.item(s);
	    if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
	      Element fstElmnt = (Element) fstNode;
	      strBillNumber = fstElmnt.getAttribute("session")+fstElmnt.getAttribute("type")+fstElmnt.getAttribute("number");
	      strBillDetails = strBillNumber + "|" + fstElmnt.getAttribute("number") + "|" + fstElmnt.getAttribute("session") + "|" + fstElmnt.getAttribute("type") + "|";
//	      System.out.println(fstElmnt.getAttribute("number"));
//	      System.out.println(fstElmnt.getAttribute("session"));
//	      System.out.println(fstElmnt.getAttribute("type"));
	    }
	  }
	  nodeList = document.getElementsByTagName("introduced");
	  for (int s = 0; s < 1; s++) {
	    Node fstNode = nodeList.item(s);
	    if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
	      Element fstElmnt = (Element) fstNode;
//	      System.out.println(fstElmnt.getAttribute("datetime"));
	      strBillDetails = strBillDetails + fstElmnt.getAttribute("datetime");
	    }
	  }
	  
	  nodeList = document.getElementsByTagName("title");
	  for (int s = 0; s < 1; s++) {
	    Node fstNode = nodeList.item(s);
	    if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
	      Element fstElmnt = (Element) fstNode;
	      NodeList fstNm = fstElmnt.getChildNodes();
//			System.out.println("Title is: " + ((Node) fstNm.item(0)).getNodeValue());
			strBillDetails = strBillDetails + "|" +  ((Node) fstNm.item(0)).getNodeValue();
	    }
	  }
      

      nodeList = document.getElementsByTagName("sponsor");
	  for (int s = 0; s < nodeList.getLength(); s++) {
	    Node fstNode = nodeList.item(s);
	    if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
	      Element fstElmnt = (Element) fstNode;
//	      System.out.println(fstElmnt.getAttribute("id"));
	      strBillDetails = strBillDetails + "|" + fstElmnt.getAttribute("id"); 
	    }
	  }
	  nodeList = document.getElementsByTagName("cosponsor");
	  for (int s = 0; s < nodeList.getLength(); s++) {
	    Node fstNode = nodeList.item(s);
	    if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
	      Element fstElmnt = (Element) fstNode;
//	      System.out.println(fstElmnt.getAttribute("id"));
//	      System.out.println(fstElmnt.getAttribute("joined"));
	      strBillCoSponsor =  strBillNumber + "|" + fstElmnt.getAttribute("id")  + "|" + fstElmnt.getAttribute("joined");
	      output2.write(strBillCoSponsor);
	      output2.newLine();
	      
	    }
	  }
	  
	  nodeList = document.getElementsByTagName("action");
	  for (int s = 0; s < nodeList.getLength(); s++) {
	    Node fstNode = nodeList.item(s);
	    if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
	      Element fstElmnt = (Element) fstNode;
//	      System.out.println(fstElmnt.getAttribute("datetime"));
//	      System.out.println(fstElmnt.getAttribute("state"));
	    }
	  }

	  nodeList = document.getElementsByTagName("committee");
	  for (int s = 0; s < nodeList.getLength(); s++) {
	    Node fstNode = nodeList.item(s);
	    if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
	      Element fstElmnt = (Element) fstNode;
//	      System.out.println(fstElmnt.getAttribute("code"));
//	      System.out.println(fstElmnt.getAttribute("activity"));
	    }
	  }
	  
	  nodeList = document.getElementsByTagName("term");
	  for (int s = 0; s < nodeList.getLength(); s++) {
	    Node fstNode = nodeList.item(s);
	    if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
	      Element fstElmnt = (Element) fstNode;
//	      System.out.println(fstElmnt.getAttribute("name"));
	      strBillSubjects =  strBillNumber + "|" + fstElmnt.getAttribute("name");
	      output3.write(strBillSubjects);
	      output3.newLine();
	    }
	  }
	  nodeList = document.getElementsByTagName("summary");
	  for (int s = 0; s < 1; s++) {
	    Node fstNode = nodeList.item(s);
	    if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
	      Element fstElmnt = (Element) fstNode;
	      NodeList fstNm = fstElmnt.getChildNodes();
//			System.out.println("Relevance is: " + ((Node) fstNm.item(0)).getNodeValue());
			strSummary = ((Node) fstNm.item(0)).getNodeValue();
			strBillDetails = strBillDetails + "|" + strSummary.replace("\n", "").replace("\r", "");
	    }
	  }
	  
	  NodeList children2 = document.getElementsByTagName( "actions");
      // .. do something ...
      try{
			
    	  for(Integer k=0; k<children2.getLength();k++){
    		   nodeList = document.getElementsByTagName("action");
    		   for (int s = 0; s < nodeList.getLength(); s++) {
    			    Node fstNode = nodeList.item(s);
    			    if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
    			      Element fstElmnt = (Element) fstNode;
    			      strActionDate = fstElmnt.getAttribute("datetime");
    			      if (fstElmnt.hasAttribute("state")){
    			    	  strActionState= fstElmnt.getAttribute("state");
    			      } else {
    			    	  strActionState = " ";
    			      }
				   //   				 
  //     			   strAction = strBillNumber + "|" + fstElmnt.getAttribute("datetime") + "|" + fstElmnt.getAttribute("state");
     			    }
        		   NodeList nodeList2 = document.getElementsByTagName("text");
    			    Node fstNode1 = nodeList2.item(s);
    			    if (fstNode1.getNodeType() == Node.ELEMENT_NODE) {
    			      Element fstElmnt2 = (Element) fstNode1;
    			      NodeList fstNm = fstElmnt2.getChildNodes();
    				  strAction = strBillNumber + "|" + strActionDate + "|" + strActionState + "|" + ((Node) fstNm.item(0)).getNodeValue();
    			    }
    		   output4.write(strAction);
    		   output4.newLine();
  			  }

    	  }
    	  
      }catch (Exception e){
			//log.error(e);
		}
      
	  output.write(strBillDetails);
      output.newLine();
      }
      }
  
      output.close();
	  output2.close();
	  output3.close();
	  output4.close();
	  System.out.println("finished");


    } catch (Exception ex) {
      ex.printStackTrace();
    }
    

  }
}