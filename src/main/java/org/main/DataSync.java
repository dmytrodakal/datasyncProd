package org.main;

import com.sforce.async.*;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Dmytro
 */
public class DataSync {

    private PartnerConnection pc;
    private DescribeSObjectResult[] desc;
    private HashMap<String, ArrayList<String>> fieldsMapping;
    private HashMap<String, ArrayList<String>> linkFieldsMapping;
    private Boolean isLender = false;
    private Boolean isPackageDeal = true;
    private BulkConnection sourceOrgConnection;
    private BulkConnection targetOrgConnection;

    private final String[] OBJECT_NAMES = {
            "Account", "Contact", "MCG_Property__c",
            "Account", "Opportunity", "Opportunity", "Deal_Quote__c",  "OpportunityTeamMember",
            "Capital_Stack__c", "Capital_Stack_Deal__c", "Loans__c","OpportunityContactRole"
    };


    private final String[] OBJECT_NAMES_TO_LINK = {
            "MCG_Property__c", "Opportunity", "Opportunity", "Loans__c"
    };

    public static void main(String[] args) {

        DataSync jap = new DataSync();
        System.out.println("Data Sync Started");
        try {
            jap.run();
        } catch (AsyncApiException ex) {
            Logger.getLogger(DataSync.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ConnectionException ex) {
            Logger.getLogger(DataSync.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(DataSync.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(DataSync.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void run()
            throws AsyncApiException, ConnectionException, IOException, InterruptedException {
        String sourceOrgPassword = System.getenv("SOURCE_ORG_PASSWORD");
        String sourceOrgUserName = System.getenv("SOURCE_ORG_USERNAME");
        String sourceOrgToken = System.getenv("SOURCE_ORG_TOKEN");
        String sourceOrgEndpointUrl = System.getenv("SOURCE_ORG_TOKEN");

        sourceOrgConnection = getBulkConnection(
                sourceOrgUserName,
                sourceOrgPassword+sourceOrgToken,
                sourceOrgEndpointUrl
        );
        prepareConfig();
        getData(sourceOrgConnection);

        String targetOrgPassword = System.getenv("TARGET_ORG_PASSWORD");
        String targetOrgUserName = System.getenv("TARGET_ORG_USERNAME");
        String targetOrgToken = System.getenv("TARGET_ORG_TOKEN");
        String targetOrgEndpointUrl = System.getenv("TARGET_ORG_TOKEN");

        targetOrgConnection = getBulkConnection(
                targetOrgUserName,
                targetOrgPassword+targetOrgToken,
                targetOrgEndpointUrl
        );
        loadData(targetOrgConnection);

    }

    private void loadData(BulkConnection bulkConnection) throws AsyncApiException, IOException, ConnectionException, InterruptedException {
        System.out.println("Loading Data");
        for (String objName : OBJECT_NAMES) {
            if (objName.equals("OpportunityContactRole")) {
                processOpportunityContactRoles();
                continue;
            }
            System.out.println(objName);
            JobInfo job;
            List<BatchInfo> batchInfoList = null;
            if (objName.equals("Account")) {
                job = createJob(objName, bulkConnection, OperationEnum.upsert, this.isLender ? "MCG_Magic_Id__c" : "Migration_Id__c");
                batchInfoList = createBatchesFromCSVFile(bulkConnection, job,
                        objName + (this.isLender ? ".lender" : ".company") + ".csv");
                this.isLender = !this.isLender;
            } else if (objName.equals("Opportunity")) {
                job = createJob(objName, bulkConnection, OperationEnum.upsert, "Migration_Id__c");
                batchInfoList = createBatchesFromCSVFile(bulkConnection, job,
                        objName + (this.isPackageDeal ? ".parent" : ".child") + ".csv");
                this.isPackageDeal = !this.isPackageDeal;
            } else {
                job = createJob(objName, bulkConnection, OperationEnum.upsert, "Migration_Id__c");
                batchInfoList = createBatchesFromCSVFile(bulkConnection, job,
                        objName + ".csv");

            }
            closeJob(bulkConnection, job.getId());
            awaitCompletion(bulkConnection, job, batchInfoList);
            checkResults(bulkConnection, job, batchInfoList);
        }
        this.linkingConfig();
        fieldsMapping = linkFieldsMapping;
        for (String objName : OBJECT_NAMES_TO_LINK) {
            List<BatchInfo> batchInfoList;
            JobInfo job = createQueryJob(objName, sourceOrgConnection);
            saveResultToFile(sourceOrgConnection, job, objName);
            closeJob(sourceOrgConnection, job.getId());
            job = createJob(objName, bulkConnection, OperationEnum.upsert, "Migration_Id__c");
            if(objName.equals("Opportunity")){
                job = createJob(objName, bulkConnection, OperationEnum.upsert, "Migration_Id__c");
                batchInfoList = createBatchesFromCSVFile(bulkConnection, job,
                        objName + (this.isPackageDeal ? ".parent" : ".child") + ".csv");
                this.isPackageDeal = !this.isPackageDeal;
            }else{
                batchInfoList = createBatchesFromCSVFile(bulkConnection, job,
                        objName + ".csv");
            }

            closeJob(bulkConnection, job.getId());
            awaitCompletion(bulkConnection, job, batchInfoList);
            checkResults(bulkConnection, job, batchInfoList);
        }
    }

    private void getData(BulkConnection bulkConnection) {
        System.out.println("Pulling Data");
        try {
            for (String objName : OBJECT_NAMES) {

                JobInfo job = createQueryJob(objName, bulkConnection);
                saveResultToFile(bulkConnection, job, objName);
                closeJob(bulkConnection, job.getId());
                if (objName.equals("Account")) {
                    this.isLender = !this.isLender;
                } else if (objName.equals("Opportunity")) {
                    this.isPackageDeal = !this.isPackageDeal;
                }
            }
        } catch (AsyncApiException aae) {
            aae.printStackTrace();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        } catch (ConnectionException ex) {
            Logger.getLogger(DataSync.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(DataSync.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private void prepareConfig() {
        fieldsMapping = new HashMap<String, ArrayList<String>>();
        ArrayList<String> accountExternalFields = new ArrayList<String>();
        accountExternalFields.add("RecordTypeId");
        accountExternalFields.add("Owner.CSR_SalesmanID__c");
        fieldsMapping.put("Account", accountExternalFields);
        //contact setup
        ArrayList<String> contactExternalFields = new ArrayList<String>();
        contactExternalFields.add("Account.Migration_ID__c");
        contactExternalFields.add("Owner.CSR_SalesmanID__c");
        contactExternalFields.add("RecordTypeId");
        fieldsMapping.put("Contact", contactExternalFields);
        //property setup
        ArrayList<String> propertyExternalFields = new ArrayList<String>();
        propertyExternalFields.add("Property_Owner__r.Migration_ID__c");
        propertyExternalFields.add("Primary_Contact__r.Migration_ID__c");
        propertyExternalFields.add("Deal_Quote__r.Migration_ID__c");
        fieldsMapping.put("MCG_Property__c", propertyExternalFields);
        //opportunity setup
        ArrayList<String> opportunityExternalFields = new ArrayList<String>();
        opportunityExternalFields.add("RecordTypeId");
        opportunityExternalFields.add("Property__r.Migration_ID__c");
        opportunityExternalFields.add("Account.Migration_ID__c");
        opportunityExternalFields.add("Deal_Admin__r.Migration_ID__c");
        opportunityExternalFields.add("Landlord_Contact__r.Migration_ID__c");
        opportunityExternalFields.add("Landlord_Company__r.Migration_ID__c");
        opportunityExternalFields.add("Main_Contact__r.Migration_ID__c");
        opportunityExternalFields.add("Primary_Broker__r.CSR_SalesmanID__c");
        opportunityExternalFields.add("Owner.CSR_SalesmanID__c");
        fieldsMapping.put("Opportunity", opportunityExternalFields);
        //Deal quotes setup
        ArrayList<String> quotesExternalFields = new ArrayList<String>();
        quotesExternalFields.add("Deal__r.Migration_ID__c");
        quotesExternalFields.add("Lender_Contact__r.Migration_ID__c");
        quotesExternalFields.add("Lender_Name__r.MCG_Magic_ID__c");
        quotesExternalFields.add("Loan_Officer__r.Migration_ID__c");
        //quotesExternalFields.add("Parent_Quote__r.Migration_ID__c");
        quotesExternalFields.add("RecordTypeId");
        fieldsMapping.put("Deal_Quote__c", quotesExternalFields);

        ArrayList<String> otmExternalFields = new ArrayList<String>();
        otmExternalFields.add("Opportunity.Migration_ID__c");
        otmExternalFields.add("User.CSR_SalesmanID__c");
        fieldsMapping.put("OpportunityTeamMember", otmExternalFields);

        ArrayList<String> capitalStackExternalFields = new ArrayList<String>();
        capitalStackExternalFields.add("Property__r.Migration_ID__c");
        capitalStackExternalFields.add("Property_Owner__r.Migration_ID__c");
        fieldsMapping.put("Capital_Stack__c", capitalStackExternalFields);

        ArrayList<String> capitalStackDealsExternalFields = new ArrayList<String>();
        capitalStackDealsExternalFields.add("Capital_Stack__r.Migration_ID__c");
        capitalStackDealsExternalFields.add("LoanCurrentOwner__r.Migration_ID__c");
        capitalStackDealsExternalFields.add("Deal__r.Migration_ID__c");
        fieldsMapping.put("Capital_Stack_Deal__c", capitalStackDealsExternalFields);

        ArrayList<String> loansExternalFields = new ArrayList<String>();
        loansExternalFields.add("MCG_Deal_Property__r.Migration_ID__c");
        loansExternalFields.add("MCG_Property__r.Migration_ID__c");
        loansExternalFields.add("MCG_Lender_Name__r.Migration_ID__c");
        loansExternalFields.add("Quote__r.Migration_ID__c");
        loansExternalFields.add("Lender_Contact__r.Migration_ID__c");
        loansExternalFields.add("Loan_Officer__r.Migration_ID__c");
        loansExternalFields.add("Prepayment_Penalty_Structure__r.Migration_ID__c");
        loansExternalFields.add("Master_Owner__r.Migration_ID__c");
        fieldsMapping.put("Loans__c", loansExternalFields);

        ArrayList<String> opportunityContactRoleExternalFields = new ArrayList<String>();
        opportunityContactRoleExternalFields.add("Opportunity.Migration_ID__c");
        opportunityContactRoleExternalFields.add("Contact.Migration_ID__c");
        fieldsMapping.put("OpportunityContactRole", opportunityContactRoleExternalFields);

        ArrayList<String> loanPrepStructurePeriodExternalFields = new ArrayList<String>();
        loanPrepStructurePeriodExternalFields.add("Prepayment_Structure__r.Migration_ID__c");
        fieldsMapping.put("Loan_Prepayment_Structure_Period__c", loanPrepStructurePeriodExternalFields);

        ArrayList<String> loanPrepPeriodExternalFields = new ArrayList<String>();
        loanPrepPeriodExternalFields.add("Loan__r.Migration_ID__c");
        fieldsMapping.put("Loan_Prepayment_Period__c", loanPrepPeriodExternalFields);

    }

    private void linkingConfig() {
        //property setup
        linkFieldsMapping = new HashMap<String, ArrayList<String>>();
        ArrayList<String> propertyExternalFields = new ArrayList<String>();
        propertyExternalFields.add("Cap_Stack__r.Migration_ID__c");
        propertyExternalFields.add("Deal_Quote__r.Migration_ID__c");
        propertyExternalFields.add("Latest_Loan__r.Migration_ID__c");
        propertyExternalFields.add("Latest_MCG_Associated_Deal__r.Migration_ID__c");
        propertyExternalFields.add("MCG_Broker_on_Assocd_Deal__r.CSR_SalesmanID__c");
        linkFieldsMapping.put("MCG_Property__c", propertyExternalFields);

        ArrayList<String> opportunityExternalFields = new ArrayList<String>();
        opportunityExternalFields.add("Capital_Stack__r.Migration_ID__c");
        opportunityExternalFields.add("Prior_Deal__r.Migration_ID__c");
        opportunityExternalFields.add("Parent_Deal__r.Migration_ID__c");
        opportunityExternalFields.add("Related_Deal__r.Migration_ID__c");
        opportunityExternalFields.add("Deal_Quote__r.Migration_ID__c");
        opportunityExternalFields.add("Selected_Loan__r.Migration_ID__c");
        opportunityExternalFields.add("Main_Contact__r.Migration_ID__c");
        linkFieldsMapping.put("Opportunity", opportunityExternalFields);

        ArrayList<String> loansExternalFields = new ArrayList<String>();
        loansExternalFields.add("Parent_Loan__r.Migration_ID__c");
        linkFieldsMapping.put("Loans__c", loansExternalFields);
    }

    private BulkConnection getBulkConnection(String userName, String password, String endpoint)
            throws ConnectionException, AsyncApiException {

        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(userName);
        partnerConfig.setPassword(password);
        partnerConfig.setAuthEndpoint(endpoint);
        pc = new PartnerConnection(partnerConfig);
        desc = pc.describeSObjects(OBJECT_NAMES);
        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConfig.getSessionId());
        String soapEndpoint = partnerConfig.getServiceEndpoint();
        String apiVersion = "57.0";
        String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/"))
                + "async/" + apiVersion;
        config.setRestEndpoint(restEndpoint);
        config.setCompression(true);
        config.setTraceMessage(false);
        BulkConnection connection = new BulkConnection(config);
        return connection;
    }

    private JobInfo createJob(String sobjectType, BulkConnection connection, OperationEnum operationType, String externalFieldName)
            throws AsyncApiException {
        JobInfo job = new JobInfo();
        job.setObject(sobjectType);
        job.setExternalIdFieldName(externalFieldName);
        job.setOperation(operationType);
        job.setContentType(ContentType.CSV);
        if (sobjectType.equals("Opportunity") || sobjectType.equals("OpportunityTeamMember") || sobjectType.equals("Capital_Stack_Deal__c") || sobjectType.equals("OpportunityContactRole")) {
            job.setConcurrencyMode(ConcurrencyMode.Serial);
        }
        job = connection.createJob(job);
        return job;
    }

    private JobInfo createQueryJob(String sobjectType, BulkConnection connection)
            throws AsyncApiException {
        JobInfo job = new JobInfo();
        job.setObject(sobjectType);
        job.setOperation(OperationEnum.query);
        job.setContentType(ContentType.CSV);
        job = connection.createJob(job);
        return job;
    }

    private void closeJob(BulkConnection connection, String jobId)
            throws AsyncApiException {
        JobInfo job = new JobInfo();
        job.setId(jobId);
        job.setState(JobStateEnum.Closed);
        connection.updateJob(job);
    }

    private void awaitCompletion(BulkConnection connection, JobInfo job,
                                 List<BatchInfo> batchInfoList)
            throws AsyncApiException {
        long sleepTime = 0L;
        Set<String> incomplete = new HashSet<String>();
        for (BatchInfo bi : batchInfoList) {
            incomplete.add(bi.getId());
        }
        while (!incomplete.isEmpty()) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
            }
            sleepTime = 15000L;
            BatchInfo[] statusList
                    = connection.getBatchInfoList(job.getId()).getBatchInfo();
            for (BatchInfo b : statusList) {
                if (b.getState() == BatchStateEnum.Completed
                        || b.getState() == BatchStateEnum.Failed) {
                    if (incomplete.remove(b.getId())) {
                        System.out.println("BATCH STATUS:\n" + b);
                    }
                }
            }
        }
    }

    private void checkResults(BulkConnection connection, JobInfo job,
                              List<BatchInfo> batchInfoList)
            throws AsyncApiException, IOException {
        for (BatchInfo b : batchInfoList) {
            CSVReader rdr
                    = new CSVReader(connection.getBatchResultStream(job.getId(), b.getId()));
            List<String> resultHeader = rdr.nextRecord();
            int resultCols = resultHeader.size();

            List<String> row;
            while ((row = rdr.nextRecord()) != null) {
                Map<String, String> resultInfo = new HashMap<String, String>();
                for (int i = 0; i < resultCols; i++) {
                    resultInfo.put(resultHeader.get(i), row.get(i));
                }
                boolean success = Boolean.valueOf(resultInfo.get("Success"));
                boolean created = Boolean.valueOf(resultInfo.get("Created"));
                String id = resultInfo.get("Id");
                String error = resultInfo.get("Error");
                if (success && created) {
                    System.out.println("Created row with id " + id);
                } else if (!success) {
                    System.out.println("Failed with error: " + error);
                }
            }
        }
    }

    private void saveResultToFile(BulkConnection bulkConnection, JobInfo job, String fileName)
            throws AsyncApiException, InterruptedException, ConnectionException, IOException {
        assert job.getId() != null;

        job = bulkConnection.getJobStatus(job.getId());
        String query = createQuery(job.getObject());

        long start = System.currentTimeMillis();

        BatchInfo info = null;
        ByteArrayInputStream bout
                = new ByteArrayInputStream(query.getBytes());
        info = bulkConnection.createBatchFromStream(job, bout);

        String[] queryResults = null;

        for (int i = 0; i < 10000; i++) {
            Thread.sleep(10000);
            info = bulkConnection.getBatchInfo(job.getId(),
                    info.getId());

            if (info.getState() == BatchStateEnum.Completed) {
                QueryResultList list
                        = bulkConnection.getQueryResultList(job.getId(),
                        info.getId());
                queryResults = list.getResult();
                break;
            } else if (info.getState() == BatchStateEnum.Failed) {
                System.out.println("-------------- failed ----------"
                        + info);
                break;
            } else {
                System.out.println("-------------- waiting ----------"
                        + info);
            }
        }

        if (queryResults != null) {
            for (String resultId : queryResults) {
                InputStream is = bulkConnection.getQueryResultStream(job.getId(),
                        info.getId(), resultId);
                if (job.getObject().equals("Account")) {
                    convertInputStreamToFile(is, job.getObject() + (this.isLender ? ".lender" : ".company"));
                } else if (job.getObject().equals("Opportunity")) {
                    convertInputStreamToFile(is, job.getObject() + (this.isPackageDeal ? ".parent" : ".child"));
                } else {
                    convertInputStreamToFile(is, fileName);
                }
            }
        }
    }

    private String createQuery(String objectName) throws ConnectionException {
        ArrayList<String> fieldsName = new ArrayList<String>();
        desc = pc.describeSObjects(new String[]{objectName});
        Field[] fields = desc[0].getFields();
        for (Field f : fields) {
            if (f.isUpdateable() && f.getType() != FieldType.reference) {
                fieldsName.add(f.getName());
            }

        }
        String query;
        System.out.println(objectName);
        if (objectName.equals("Account")) {
            String extIdField = this.isLender ? "MCG_Magic_ID__c" : "Migration_Id__c";
            query = "SELECT " + String.join(",", fieldsMapping.get(objectName)) + ","
                    + String.join(",", fieldsName)
                    + " FROM " + objectName + " WHERE " + extIdField + "!=null";
        } else if (objectName.equals("Opportunity")) {
            query = "SELECT " + String.join(",", fieldsMapping.get(objectName)) + ","
                    + String.join(",", fieldsName)
                    + " FROM " + objectName + " WHERE Migration_Id__c!=null AND Parent_Deal__c" + (this.isPackageDeal ? "=null" : "!=null");
        } else if (objectName.equals("OpportunityContactRole")) {
            query = "SELECT Id," + String.join(",", fieldsMapping.get(objectName))
                    + "," + String.join(",", fieldsName)
                    + " FROM " + objectName + " WHERE Migration_Id__c!=null";
        } else {
            if (fieldsMapping.containsKey(objectName)) {
                query = "SELECT " + String.join(",", fieldsMapping.get(objectName))
                        + "," + String.join(",", fieldsName)
                        + " FROM " + objectName + " WHERE Migration_Id__c!=null";
            } else {
                query = "SELECT " + String.join(",", fieldsName)
                        + " FROM " + objectName + " WHERE Migration_Id__c!=null";
            }
        }

        return query;
    }

    public static void convertInputStreamToFile(InputStream is, String fileName) throws IOException {
        OutputStream outputStream = null;
        try {
            File file = new File(fileName + ".csv");
            outputStream = new FileOutputStream(file);

            int read = 0;
            byte[] bytes = new byte[1024];
            while ((read = is.read(bytes)) != -1) {
                outputStream.write(bytes, 0, read);
            }
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
    }

    private List<BatchInfo> createBatchesFromCSVFile(BulkConnection connection,
                                                     JobInfo jobInfo, String csvFileName)
            throws IOException, AsyncApiException {
        List<BatchInfo> batchInfos = new ArrayList<BatchInfo>();
        BufferedReader rdr = new BufferedReader(
                new InputStreamReader(new FileInputStream(csvFileName))
        );
        byte[] headerBytes = (rdr.readLine() + "\n").getBytes("UTF-8");
        int headerBytesLength = headerBytes.length;
        File tmpFile = File.createTempFile("bulkAPIInsert", ".csv");

        try {
            FileOutputStream tmpOut = new FileOutputStream(tmpFile);
            int maxBytesPerBatch = 10000000;
            int maxRowsPerBatch = 10000;
            int currentBytes = 0;
            int currentLines = 0;
            String nextLine;
            while ((nextLine = rdr.readLine()) != null) {
                byte[] bytes = (nextLine + "\n").getBytes("UTF-8");
                if (currentBytes + bytes.length > maxBytesPerBatch
                        || currentLines > maxRowsPerBatch) {
                    createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
                    currentBytes = 0;
                    currentLines = 0;
                }
                if (currentBytes == 0) {
                    tmpOut = new FileOutputStream(tmpFile);
                    tmpOut.write(headerBytes);
                    currentBytes = headerBytesLength;
                    currentLines = 1;
                }
                tmpOut.write(bytes);
                currentBytes += bytes.length;
                currentLines++;
            }
            if (currentLines > 1) {
                createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
            }
        } finally {
            tmpFile.delete();
        }
        return batchInfos;
    }

    private void createBatch(FileOutputStream tmpOut, File tmpFile,
                             List<BatchInfo> batchInfos, BulkConnection connection, JobInfo jobInfo)
            throws IOException, AsyncApiException {
        tmpOut.flush();
        tmpOut.close();
        FileInputStream tmpInputStream = new FileInputStream(tmpFile);
        try {
            BatchInfo batchInfo
                    = connection.createBatchFromStream(jobInfo, tmpInputStream);
            System.out.println(batchInfo);
            batchInfos.add(batchInfo);

        } finally {
            tmpInputStream.close();
        }
    }

    private void processOpportunityContactRoles() throws AsyncApiException, IOException, ConnectionException, InterruptedException {
        System.out.println("Retrieve OpportunityContactRole from target org");
        JobInfo job = createQueryJob("OpportunityContactRole", targetOrgConnection);
        saveResultToFile(targetOrgConnection, job, "OpportunityContactRole.target");
        closeJob(targetOrgConnection, job.getId());

        BufferedReader br = new BufferedReader(new FileReader("OpportunityContactRole.target.csv"));
        String line = null;

        Map<String, String> idToIdMapping = new HashMap<>();

        Map<String, String> externalIdsMappingTarget = new HashMap<>();

        int counter = 0;
        String keys[] = null;
        while ((line = br.readLine()) != null) {
            Map<String, String> map = new HashMap<String, String>();
            if (counter == 0) {
                keys = line.split(",");
            }
            if (counter > 0) {
                String str[] = line.split(",");
                for (int i = 0; i < str.length; i++) {
                    map.put(keys[i].replace("\"", ""), str[i].replace("\"", ""));
                }
            }
            externalIdsMappingTarget.put(map.get("Migration_ID__c"), map.get("Id"));
            counter++;
        }

        counter = 0;
        Map<String, String> externalIdsMappingSource = new HashMap<>();
        BufferedReader br2 = new BufferedReader(new FileReader("OpportunityContactRole.csv"));
        while ((line = br2.readLine()) != null) {
            Map<String, String> map = new HashMap<String, String>();
            if (counter == 0) {
                keys = line.split(",");
            }
            if (counter > 0) {
                String str[] = line.split(",");
                for (int i = 0; i < str.length; i++) {
                    map.put(keys[i].replace("\"", ""), str[i].replace("\"", ""));
                }
            }
            externalIdsMappingSource.put(map.get("Migration_ID__c"), map.get("Id"));
            counter++;
        }

        for (String key : externalIdsMappingSource.keySet()) {
            if (externalIdsMappingTarget.containsKey(key)) {
                idToIdMapping.put(externalIdsMappingSource.get(key), externalIdsMappingTarget.get(key));
            } else {
                idToIdMapping.put(externalIdsMappingSource.get(key), "");
            }
        }
        idToIdMapping.remove("Id");
        Scanner sc = new Scanner(new File("OpportunityContactRole.csv"));
        File tempFile = new File("OpportunityContactRole.temp.csv");
        tempFile.createNewFile();
        FileWriter fw = new FileWriter(tempFile);
        StringBuilder sb = new StringBuilder();
        counter = 0;
        while (sc.hasNext()) {
            if (counter == 0) {
                counter++;
                continue;
            }
            String[] words = sc.nextLine().split(",");
            for (String word : words) {
                word = word.replace("\"", "");
                if (idToIdMapping.containsKey(word)) {
                    word = idToIdMapping.get(word);
                }
                sb.append("\"" + word + "\"").append(",");
            }
            counter++;
            sb.deleteCharAt(sb.lastIndexOf(","));
            sb.append("\n");
        }
        fw.write(sb.toString());
        fw.close();
        List<BatchInfo> batchInfoList = null;
        job = createJob("OpportunityContactRole", this.targetOrgConnection, OperationEnum.upsert, "Id");
        batchInfoList = createBatchesFromCSVFile(this.targetOrgConnection, job, "OpportunityContactRole.temp.csv");
        closeJob(this.targetOrgConnection, job.getId());
        awaitCompletion(this.targetOrgConnection, job, batchInfoList);
        checkResults(this.targetOrgConnection, job, batchInfoList);
    }

}
