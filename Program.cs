using System.Data.SqlClient;
using CsvHelper;
using System.Data;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.Management.Smo;

try
{

    string[] arguments = Environment.GetCommandLineArgs();
    //PARAMETERIZE client,environment

    //build req list from filters if needed
    //build candidate list from filters if needed
    //build candprofile list from filters if needed

    string client = arguments[1].ToString();
    string environment = arguments[2].ToString();
    string attachmentFlag = arguments[3].ToString();
    string obfuscateFlag = arguments[4].ToString();
    string filterFlag = arguments[5].ToString();
    string filtertypes = arguments[6].ToString();
    string filterValues = arguments[7].ToString();
    string filePath = arguments[8].ToString();
    
    

    //req status
    //change of reqstatus since date
    //new cand application since date



    //string configFile = arguments[3].ToString();

    string clientServer = "";
    string organizationCode = "";

    //clientServer = getServer(environment, client);


    string server = "";
    RestoreDatabases(client, out server, environment);
    GetOrganizationCode(client, environment,out organizationCode);

    if (filterFlag == "Y")
    {
        buildTempData(client + "_Export", filtertypes, filterValues, server,environment);
    }
    string emailClient = client;
    client += "_Export";
    
    SqlConnectionStringBuilder sqlconnstr = new SqlConnectionStringBuilder();
    sqlconnstr.InitialCatalog = client;
    sqlconnstr.DataSource = server;
    sqlconnstr.IntegratedSecurity = true;
    sqlconnstr.MultipleActiveResultSets = true;

    SqlConnection sqlconn = new SqlConnection(sqlconnstr.ConnectionString);
    sqlconn.Open();

    SqlCommand mappingCmd = sqlconn.CreateCommand();

    //build cand and req case fields for mappable fields like gender,source,status 
    mappingCmd.CommandText = @"select [RMS Field] as rmsfield,ID,[PFR Value] as pfrvalue from ['Candidate Data Mapping (RMS - P$']";

    SqlDataReader sqlDataReader = mappingCmd.ExecuteReader();
    string genderCase = " CASE isnull(c.gendertypeid,1) ";
    string raceCase = " CASE isnull(ce.RaceTypeID,1) ";
    string sourceCase = " CASE cp.SourceReferringTypeID ";
    string disabilityCase = " CASE isnull(pods.PreOfferDisabilityStatusID,0) ";
    string veteranCase = " CASE isnull(povs.PreOfferVeteranStatusID,0) ";
    while (sqlDataReader.Read())
    {
        if (sqlDataReader["rmsfield"].ToString() == "OFCCPPreOfferDisabilityStatus")
        {

            disabilityCase += @" WHEN " + sqlDataReader["ID"].ToString() + @" THEN '" + sqlDataReader["pfrvalue"].ToString().Replace("'","''") + "'";

        }
        if (sqlDataReader["rmsfield"].ToString() == "OFCCPVeteranStatus")
        {

            veteranCase += @" WHEN " + sqlDataReader["ID"].ToString() + @" THEN '" + sqlDataReader["pfrvalue"].ToString().Replace("'", "''") + "'";

        }
        if (sqlDataReader["rmsfield"].ToString() == "SourceReferringType")
        {

            sourceCase += @" WHEN " + sqlDataReader["ID"].ToString() + @" THEN '" + sqlDataReader["pfrvalue"].ToString().Replace("'", "''") + "'";

        }
        if (sqlDataReader["rmsfield"].ToString() == "Race")
        {

            raceCase += @" WHEN " + sqlDataReader["ID"].ToString() + @" THEN '" + sqlDataReader["pfrvalue"].ToString().Replace("'", "''") + "'";

        }
        if (sqlDataReader["rmsfield"].ToString() == "Gender")
        {

            genderCase += @" WHEN " + sqlDataReader["ID"].ToString() + @" THEN '" + sqlDataReader["pfrvalue"].ToString().Replace("'", "''") + "'";

        }
        


    }
    genderCase += " end as GENDER, ";
    raceCase += " end as Ethnicity, ";
    veteranCase += " end as VeteranStatus,";
    disabilityCase += " end as DISABILITY,";
    sourceCase += " end as source ";

    if (sourceCase == " CASE cp.SourceReferringTypeID " + " end as source ")
    {

        sourceCase = " NULL as source ";
    }

    sqlDataReader.Close();

    string reqVariables = "";
    string reqDept = "";
    string reqSalaryMin ="";
    string reqSalaryMax = "";

    mappingCmd.CommandText = @"select [FieldDef Code] as code,area,fieldname from ['Req - Core to Core$'] where [Include For Client?] = 'Yes'  
and FieldName in ('MaxSalary',
'MinSalary','Department')";
    SqlDataReader reqCustomorCore = mappingCmd.ExecuteReader();
    while (reqCustomorCore.Read())
    {
       // Console.WriteLine(reqCustomorCore["fieldname"].ToString());
       // Console.WriteLine(reqCustomorCore["area"].ToString());


        if (reqCustomorCore["fieldname"].ToString() == "Department" && reqCustomorCore["area"].ToString() == "Requisition Custom")
        {
            reqDept += @"cfv.Department,";
        }
        if (reqCustomorCore["fieldname"].ToString() == "MinSalary" && reqCustomorCore["area"].ToString() == "Requisition Custom")
        {
            reqSalaryMin += @"cfv.MinSalary as SalaryLow,";
        }
        if (reqCustomorCore["fieldname"].ToString() == "MaxSalary" && reqCustomorCore["area"].ToString() == "Requisition Custom")
        {
            reqSalaryMax += @"cfv.MaxSalary as SalaryHigh";
        }

    }
    reqCustomorCore.Close();

    if (reqDept == "")
    {
        reqVariables += @"NULL as Department,";
    }
    else
    {
        reqVariables += reqDept;

    }
    if (reqSalaryMin == "")
    {

        reqVariables += @"r.SalaryLow as SalaryLow,";
    }
    else
    {
        reqVariables += reqSalaryMin;
    }
    if (reqSalaryMax == "")
    {

        reqVariables += @"r.SalaryHigh as SalaryHigh";
    }
    else
    {
        reqVariables += reqSalaryMax;

    }



    string statusCase = " CASE r.ReqStatusID ";
    string employmentTypeCase = " CASE r.EmpDurationTypeID ";
    string jobCategoryCase = " CASE rfv.PositionCategory ";
    string educationCase = " CASE r.EducationTypeID ";
    string experienceCase = " CASE r.ExperienceTypeID ";

    mappingCmd.CommandText = @"select [RMS Field] as rmsfield,ID,[PFR Value] as pfrvalue from  ['Requisition Data Mapping (RMS -$']";

    SqlDataReader mappingReader = mappingCmd.ExecuteReader();

    while (mappingReader.Read())
    {
        if (mappingReader["rmsfield"].ToString() == "Experience")
        {

            experienceCase += @" WHEN " + mappingReader["ID"].ToString() + @" THEN '" + mappingReader["pfrvalue"].ToString().Replace("'", "''") + "'";

        }
        if (mappingReader["rmsfield"].ToString() == "Education")
        {

            educationCase += @" WHEN " + mappingReader["ID"].ToString() + @" THEN '" + mappingReader["pfrvalue"].ToString().Replace("'", "''") + "'";

        }
        if (mappingReader["rmsfield"].ToString() == "JobCategory")
        {

            jobCategoryCase += @" WHEN " + mappingReader["ID"].ToString() + @" THEN '" + mappingReader["pfrvalue"].ToString().Replace("'", "''") + "'";

        }

        if (mappingReader["rmsfield"].ToString() == "EmploymentType")
        {

            employmentTypeCase += @" WHEN " + mappingReader["ID"].ToString() + @" THEN '" + mappingReader["pfrvalue"].ToString().Replace("'", "''") + "'";

        }
        if (mappingReader["rmsfield"].ToString() == "ReqStatus")
        {

            statusCase += @" WHEN " + mappingReader["ID"].ToString() + @" THEN '" + mappingReader["pfrvalue"].ToString().Replace("'", "''") + "'";

        }




    }

    
    statusCase += @" end as ReqStatus,";
    jobCategoryCase += @" end as POSITIONCATEGORY,";
    educationCase += @" end  as EDUCATION, ";
    experienceCase += @" end  as EXPIERENCE,";
    employmentTypeCase += @" end as POSITIONTYPE,";

    //candidate standard fields but need folder mappings
    //Standard portion of the candidate query part 1
    //folder mapping case statements will be generated from configuration spreadsheet and appended to this
    string baseCase = @"CASE pw.DisplayText ";
    string workflowCase = baseCase;
    string workflowCodeCase = baseCase;
    //string candQuery1 = @"select distinct cast(c.CandID as nvarchar(10))  as CANDID,
    //            + cast(cp.CandProfileID as nvarchar(10))  as CANDPROFILEID, replace(c.address1, '""', '\""') + ' ' 
    //            + replace(isnull(c.Address2, ''), '""', '\""') + ' ' + replace(isnull(c.City, ''), '""', '\""') + ' ' 
    //            + isnull(c.StateProvince, '') + ' ' + rtrim(isnull(c.postalcode, '')) + ' ' + isnull(rtrim(country.codeiso3), '') 
    //             as ADDRESS1, Format(cp.CreatedOn, 'yyyy-MM-dd hh:mm:ss tt', 'en-us')  as CREATEDON,
    //            replace(c.PrimaryEmailAddress, '""', '\""')  as PRIMARYEMAILADDRESS, replace(FirstName, '""', '\""') 
    //            + ' ' + replace(isnull(MiddleName, ''), '""', '\""') + ' ' + replace(LastName, '""', '\""')  as NAME,
    //            cast(rtrim(r.ClientReqID) as nvarchar(30)) as CLIENTREQID,cp.SourceAvenueValue 
    //             as SOURCEAVENUEVALUE, ";
    string whereClause = @"";
    string candQuery1 = @"select distinct cast(c.CandID as nvarchar(10))  as CANDID,
                + cast(cp.CandProfileID as nvarchar(10))  as CANDPROFILEID, c.address1 + ' ' 
                + c.Address2 + ' ' + isnull(c.City, '') + ' ' 
                + isnull(c.StateProvince, '') + ' ' + rtrim(isnull(c.postalcode, '')) + ' ' + isnull(rtrim(country.codeiso3), '') 
                 as ADDRESS1, Format(cp.CreatedOn, 'yyyy-MM-dd hh:mm:ss tt', 'en-us')  as CREATEDON,
                c.PrimaryEmailAddress  as PRIMARYEMAILADDRESS, FirstName 
                + ' ' + isnull(MiddleName, '') + ' ' + LastName  as NAME,
                cast(rtrim(r.ClientReqID) as nvarchar(30)) as CLIENTREQID,cp.SourceAvenueValue 
                 as SOURCEAVENUEVALUE, ";

    if (obfuscateFlag == "Y")
    {
        candQuery1 = @"select distinct cast(c.CandID as nvarchar(10))  as CANDID,
                + cast(cp.CandProfileID as nvarchar(10))  as CANDPROFILEID, cast(c.CandID as nvarchar(10)) + ' ' 
                + cast(c.CandID as nvarchar(10)) + ' ' + isnull(c.City, '') + ' ' 
                + isnull(c.StateProvince, '') + ' ' + rtrim(isnull(c.postalcode, '')) + ' ' + isnull(rtrim(country.codeiso3), '') 
                 as ADDRESS1, Format(cp.CreatedOn, 'yyyy-MM-dd hh:mm:ss tt', 'en-us')  as CREATEDON,
                cast(c.CandID as nvarchar(10))  as PRIMARYEMAILADDRESS, cast(c.CandID as nvarchar(10))
                + ' ' + cast(c.CandID as nvarchar(10)) + ' ' + cast(c.CandID as nvarchar(10))  as NAME,
                cast(rtrim(r.ClientReqID) as nvarchar(30)) as CLIENTREQID,cp.SourceAvenueValue 
                 as SOURCEAVENUEVALUE, ";

    }

    // Grab workflows
    SqlCommand sqlcmd = sqlconn.CreateCommand();
    sqlcmd.CommandText = @"select distinct [WorkFlow Display Text] as wfdt,[PFR Pipeline] as pipeline from ['Workflow Mapping$'] where [WorkFlow Display Text] is not null";

    SqlDataReader wfReader = sqlcmd.ExecuteReader();

    while (wfReader.Read())
    {

        string folderCase = @" When '" + wfReader["wfdt"].ToString() + @"' THEN CASE pf.DisplayText ";
        baseCase += folderCase;
        workflowCase += @" WHEN '" + wfReader["wfdt"].ToString() + @"' THEN '" + wfReader["pipeline"].ToString() +"'";

            
        SqlCommand folderCmd = sqlconn.CreateCommand();
        folderCmd.CommandText = @"select distinct [FolderName],[Pipeline Stage] from ['Workflow Mapping$'] where [WorkFlow Display Text] = '" + wfReader["wfdt"].ToString() + "'";

        SqlDataReader folderReader = folderCmd.ExecuteReader();

        while (folderReader.Read()) 
        {

            baseCase += " WHEN '" + folderReader["FolderName"].ToString() + "' THEN '" + folderReader["Pipeline Stage"].ToString() + "' ";

        
        }
        baseCase += @" end ";
        //Console.WriteLine(baseCase);

       

    }
    workflowCodeCase = workflowCase;
    workflowCase += @" end as WORKFLOWDISPLAYTEXT,";
    workflowCodeCase += @" end as WORKFLOW,";
    candQuery1 += baseCase;
    candQuery1 += @" end as FOLDER,";
    candQuery1 += baseCase;
    candQuery1 += @" end as FOLDERDISPLAYTEXT,";
    candQuery1 += @" Format(rfcp.CreatedOn,'yyyy - MM - dd hh: mm: ss tt','en-us') as MODIFIEDON,";
    candQuery1 += workflowCodeCase;
    candQuery1 += workflowCase;
    if (obfuscateFlag == "Y")
    {

        candQuery1 += @" cast(c.CandID as nvarchar(10))  as PHONE,  ";
    }
    else
    {
        candQuery1 += @" isnull(p.Phone,'')  as PHONE,  ";
    }
    //need requirements spreadsheet updated for the below fields
    candQuery1 += genderCase;
    //gender
    candQuery1 += raceCase;
    //ethnicity
    candQuery1 += veteranCase;
    //preoffer veteran
    candQuery1 += disabilityCase;
    //preoffer disability
    candQuery1 += sourceCase;
    //source
    //INCLUDE filter 
    candQuery1 += @"from tbl_candidate as c with (nolock)
inner join tbl_candprofile as cp with (nolock) on c.CandID = cp.candid
inner join tbl_ReqFolderCandProf as rfcp with (nolock) on cp.CandProfileID = rfcp.CandProfileID
inner join tbl_Requisition as r with (nolock) on rfcp.ReqID = r.ReqID
inner join tbl_folder as f with (nolock) on rfcp.FolderID = f.FolderID
inner join tbl_pfolder as pf with (nolock) on rfcp.FolderID = pf.FolderID and pf.pid = 1
inner join tbl_workflow as w with (nolock) on r.workflowid = w.workflowid 
inner join tbl_pworkflow as pw with (nolock) on w.workflowid = pw.workflowid and pw.pid = 1
left outer join ( select candid,max(phoneid) as maxphone from tbl_CandidatePhone  with (nolock) where IsPrimary = 1 group by candid) as cph on c.CandID = cph.CandID 
left outer join tbl_phone as p with (nolock) on cph.maxphone = p.PhoneID
left outer join Common40.dbo.tbl_country as country with (nolock) on c.CountryID = country.CountryID
left outer join tbl_CandRace as ce with (nolock) on c.CandID = ce.CandID
left outer join tbl_PPreOfferDisabilityStatus as pods with (nolock) on c.PreOfferDisabilityStatusID = pods.PreOfferDisabilityStatusID and pods.pid = 1
left outer join tbl_PPreOfferVeteranStatus as povs with (nolock) on c.PreOfferVeteranStatusID = povs.PreOfferVeteranStatusID and povs.pid = 1
";

if (filterFlag == "Y")
    {

        candQuery1 += @" inner join tempcandprofilelist as tcpl on cp.candprofileid = tcpl.candprofileid ";
    }


whereClause = "where r.reqid <> 1";

    candQuery1 += whereClause;


   candExport(candQuery1,client,server,filePath);
    //employment

    string employmentSQL = @"
        select cast(cp.CandID as nvarchar(10)) as CANDID, cast(cp.CandProfileID as nvarchar(10)) 
        as CANDPROFILEID ,rtrim(CompanyNameFactCode) as COMPANYNAME ,
     Format(EmploymentStartDate, 'yyyy-MM-dd hh:mm:ss tt', 'en-us')  as EMPLOYMENTSTARTDATE,
     Format(EmploymentEndDate, 'yyyy-MM-dd hh:mm:ss tt')  as EMPLOYMENTENDDATE
    from tbl_CandProfile as cp with (nolock)
    inner join tbl_CandProfEmployment as cep with (nolock) on cp.CandProfileID = cep.CandProfileID

    ";
    whereClause = " where IsStatic = 0";
    if (filterFlag == "Y")
    {

        employmentSQL += @" inner join tempcandprofilelist as tcpl on cp.candprofileid = tcpl.candprofileid ";
    }

   candEmploymentExport(employmentSQL,client,server,filePath);
    employmentSQL += whereClause;
    //education
    string educationSQL = @"
        select  cast(cp.candid as nvarchar(10)) as CANDID, cast(cp.CandProfileID as nvarchar(10)) 
        as CANDPROFILEID, rtrim(CollegeNameFactCode)  as COLLEGENAME,
        rtrim(CollegeDegreeFactCode) as COLLEGEDEGREE,
        rtrim(CollegeMajorFactCode) as COLLEGEMAJOR, 
        Format(GraduationDate, 'yyyy-MM-dd hh:mm:ss tt') as GRADUATIONDATE
        from tbl_CandProfile as cp with (nolock)
        inner join tbl_CandProfEducation as cep with (nolock) on cp.CandProfileID = cep.CandProfileID    
        ";

    
    whereClause = " where IsStatic = 0";
    if (filterFlag == "Y")
    {

        educationSQL += @" inner join tempcandprofilelist as tcpl on cp.candprofileid = tcpl.candprofileid ";
    }

    educationSQL += whereClause;
   candEducationExport(educationSQL, client, server,filePath);

    //requisition
    //BASE query

    string reqBaseQuery = @"
        select distinct rtrim(r.ClientReqID) as CLIENTREQID,Format(r.createdon, 'yyyy - MM - dd hh: mm: ss tt', 'en-us') as CREATEDON,
    u.FirstName + ' ' + u.LastName  as CREATE_USER,cast(r.Description as nvarchar(max)) as DESCRIPTION,
    et.DisplayText as EducationType,
    cast(RecruitStartDate as nvarchar(20)) as RECRUITSTARTDATE,cast(RecruitEndDate as nvarchar(20)) as RECRUITENDDATE,
    pl.City as CITY,l.CountryCode as COUNTRYCODE,pl.DisplayText as LOCATIONDISPLAYTEXT,pl.StateProvince as STATEPROVINCE,
    Title  as TITLE,
    ";


    //workflow --- use previous created
    reqBaseQuery += workflowCase;

    reqBaseQuery += @" et.displaytext as EDUCATIONDISPLAYTEXT, Format(r.modifiedon, 'yyyy-MM-dd hh:mm:ss tt', 'en-us') as MODIFIEDON,";

    reqBaseQuery += statusCase;
    //reqstatus
    reqBaseQuery += educationCase;
    //education
    reqBaseQuery += experienceCase;
    //experience
    reqBaseQuery += employmentTypeCase;
    //position type
    reqBaseQuery += jobCategoryCase;
    //position category

    // build the custom to core portion
    //check for custom to core. does department exist map otherwise null
    // custom salary? use custom otherwise use core



    reqBaseQuery += reqVariables;
    reqBaseQuery += @"    from tbl_Requisition as r with (nolock)
    inner join tbl_PWorkflow as pw with (nolock) on r.WorkflowID = pw.WorkflowID and pw.PID = 1
    inner join tbl_ReqFieldValue as rfv with (nolock)on r.reqid = rfv.ReqID
    inner join tbl_ReqFieldValueFreeText as rft with (nolock) on r.reqid = rft.reqid
    --left outer join ctbl_PDepartment as pd with (nolock) on rfv.Department = pd.DepartmentID and pd.pid = 1
    inner join tbl_user as u with (nolock) on r.CreatedByUserID = u.userid
    left outer join tbl_pEducationType as et with (nolock) on r.EducationTypeID = et.EducationTypeID and et.PID = 1
    inner join tbl_location as l with (nolock) on r.LocationID = l.LocationID
    inner join tbl_plocation as pl with (nolock) on l.LocationID = pl.LocationID and pl.pid = 1
    --inner join tbl_pworkflow as pw with (nolock) on r.WorkflowID = pw.WorkflowID and pw.PID = 1
    left outer join tbl_PEmpDurationType as ped with (nolock) on r.EmpDurationTypeID = ped.EmpDurationTypeID and ped.PID = 1
    inner join tbl_PReqStatus as prs with (nolock) on r.ReqStatusID = prs.ReqStatusID and prs.pid = 1
    inner join " + client + "_Report.dbo.tbl_ReqCustfieldDim_BU1 as cfv on r.reqid = cfv.reqid";

    whereClause = " where r.reqid <> 1";

    if (filterFlag == "Y")
    {

        reqBaseQuery += @" inner join tempreqlist as trl on r.reqid = trl.reqid ";
    }

    reqBaseQuery += whereClause;

    reqExport(reqBaseQuery, client, server,filePath);


    string slfBaseQuery = @"
select 'Select pb.' + cdpb.Code + ' as ParentRMSID,pb.code as ParentRMSCode, pg.displaytext as Parent,cb.' + cdcb.code + ' as ChildRMSID,cb.code as ChildRMSCode,
cg.displaytext as Child, pp.displaytext  as ParentValue,pb.isactive as ParentActive,
cp.displaytext as ChildValue,cb.isactive as ChildActive,
''Sublist'' as RelationshipType
from tbl_FieldDefHierarchy as fdh 
inner join tbl_fielddefgenericcrossreference as fdgcr on fdh.fielddefhierarchyid = fdgcr.fielddefhierarchyid
inner join ' + tdpb.code + ' as pb on fdgcr.parentkey = pb.' + cdpb.code + '  
inner join ' + tdpp.code + ' as pp on pb.' + cdpb.code + ' = pp. ' + cdpb.code + '
inner join ' + tdcb.code + ' as cb on fdgcr.childkey = cb.' + cdcb.code + '  
inner join ' + tdcp.code + ' as cp on cb.' + cdcb.code + ' = cp. ' + cdcb.code + '
inner join tbl_fielddef as pf on fdh.parentfielddefid = pf.fielddefid 
inner join tbl_fielddef as cf on fdh.childfielddefid = cf.fielddefid
inner join tbl_pguidef as pg on fdh.ParentFieldDefID = pg.FieldDefID and pg.pid = 1
inner join tbl_pguidef as cg on fdh.ChildFieldDefID = cg.FieldDefID and cg.pid = 1
where fdh.fielddefhierarchyid = ' + cast(fdh.fielddefhierarchyid as nvarchar(10)) + ' and pp.pid = 1 and cp.pid = 1
union all
Select pb.' + cdpb.Code + ' as ParentRMSID,pb.code as ParentRMSCode, cj.DisplayText as Parent,NULL as ChildRMSID,'''' as ChildRMSCode,
'''' as Child, pp.displaytext  as ParentValue,pb.isactive as ParentActive,
'''' as ChildValue,'''' as ChildActive,
''Sublist'' as RelationshipType
from ' + tdpb.code + ' as pb 
inner join ' + tdpp.code + ' as pp on pb.' + cdpb.code + ' = pp. ' + cdpb.code + ' and pp.pid = 1
left outer join tbl_FieldDefGenericCrossReference as fdgcr on  pb.' + cdpb.code + ' = fdgcr.ParentKey and fdgcr.fielddefhierarchyid = ' + cast(fdh.fielddefhierarchyid as nvarchar(10)) + '
cross join (select displaytext from tbl_FieldDefHierarchy as fdh inner join tbl_PGUIDef as pg on fdh.ParentFieldDefID = pg.FieldDefID and pg.pid = 1 where fdh.FieldDefHierarchyID = ' + cast(fdh.fielddefhierarchyid as nvarchar(10)) + ') as cj
where fdgcr.ChildKey is null 
union all
Select NULL as ParentRMSID,'''' as ParentRMSCode, '''' as Parent,cb.' + cdcb.code + ' as ChildRMSID,cb.code as ChildRMSCode,
cj.DisplayText as Child, ''''  as ParentValue,'''' as ParentActive,
cp.displaytext as ChildValue,cb.isactive as ChildActive,
''Sublist'' as RelationshipType
from ' + tdcb.code + ' as cb 
inner join ' + tdcp.code + ' as cp on cb.' + cdcb.code + ' = cp. ' + cdcb.code + ' and cp.pid = 1
left outer join tbl_FieldDefGenericCrossReference as fdgcr on  cb.' + cdcb.code + ' = fdgcr.ChildKey and fdgcr.fielddefhierarchyid = ' + cast(fdh.fielddefhierarchyid as nvarchar(10)) + '
cross join (select displaytext from tbl_FieldDefHierarchy as fdh inner join tbl_PGUIDef as pg on fdh.ChildFieldDefID = pg.FieldDefID and pg.pid = 1 where fdh.FieldDefHierarchyID = ' + cast(fdh.fielddefhierarchyid as nvarchar(10)) + ') as cj
where fdgcr.ChildKey is null 


'
from tbl_FieldDefHierarchy as fdh
inner join tbl_FieldDef as fdp on fdh.ParentFieldDefID = fdp.FieldDefID
inner join tbl_FieldDef as fdc on fdh.ChildFieldDefID = fdc.FieldDefID
inner join tbl_pguidef as pc on fdc.FieldDefID = pc.FieldDefID
inner join tbl_DomainListTable as dltp on fdp.DomainListID = dltp.domainlistid
inner join tbl_DomainListTable as dltp2 on fdp.DomainListID = dltp2.domainlistid
inner join tbl_DomainListTable as dltc on fdc.DomainListID = dltc.domainlistid
inner join tbl_DomainListTable as dltc2 on fdc.DomainListID = dltc2.domainlistid
inner join tbl_TableDef as tdpb on dltp.TableDefCode = tdpb.Code and tdpb.TableTypeID = 1
inner join tbl_TableDef as tdpp on dltp2.TableDefCode = tdpp.Code and tdpp.TableTypeID = 2
inner join tbl_TableDef as tdcb on dltc.TableDefCode = tdcb.Code and tdcb.TableTypeID = 1
inner join tbl_TableDef as tdcp on dltc2.TableDefCode = tdcp.Code and tdcp.TableTypeID = 2
inner join tbl_ColumnDef as cdpb on tdpb.Code = cdpb.TableDefCode and cdpb.IsPrimaryKey = 1
inner join tbl_ColumnDef as cdcb on tdcb.Code = cdcb.TableDefCode and cdcb.IsPrimaryKey = 1
where fdh.CrossReferenceTypeID = 2  and pc.pid = 1 and fdp.dataarea = 'REQ_CORE' and cdpb.Code != 'BCAccountID'";

    string APSLFQueries = getAPSLFQueries(slfBaseQuery,client,server,0);

    string apBaseQuery = @"select 'Select pb.' + cdpb.Code + ' as ParentRMSID,pb.code as ParentRMSCode, pg.displaytext as Parent,pb.' + cdpb.code + ' as ChildRMSID,pb.code as ChildRMSCode,
cg.displaytext as Child, pp.displaytext  as ParentValue,pb.isactive as ParentActive,
cast(pp.' + fdapc.columndefcode +' as nvarchar(max)) as ChildValue,pb.isactive as ChildActive,
''AutoPop'' as RelationshipType
from tbl_FieldDefHierarchy as fdh 
inner join tbl_FieldDefAutoPopulate as fdap on fdh.FieldDefHierarchyID = fdap.FieldDefHierarchyID
inner join tbl_FieldDefAutoPopulateColumn as fdapc on fdap.FieldDefAutoPopulateID = fdapc.FieldDefAutoPopulateID
cross join ' + tdpb.code + ' as pb 
inner join ' + tdpp.code + ' as pp on pb.' + cdpb.code + ' = pp. ' + cdpb.code + ' and pp.pid = 1 
inner join tbl_fielddef as pf on fdh.parentfielddefid = pf.fielddefid 
inner join tbl_fielddef as cf on fdh.childfielddefid = cf.fielddefid
inner join tbl_pguidef as pg on fdh.ParentFieldDefID = pg.FieldDefID and pg.pid = 1
inner join tbl_pguidef as cg on fdh.ChildFieldDefID = cg.FieldDefID and cg.pid = 1
where fdh.fielddefhierarchyid = ' + cast(fdh.fielddefhierarchyid as nvarchar(10)) + '
'
from tbl_FieldDefHierarchy as fdh
inner join tbl_FieldDef as fdp on fdh.ParentFieldDefID = fdp.FieldDefID
inner join tbl_FieldDef as fdc on fdh.ChildFieldDefID = fdc.FieldDefID
inner join tbl_FieldDefAutoPopulate as fdap on fdh.FieldDefHierarchyID = fdap.FieldDefHierarchyID
inner join tbl_FieldDefAutoPopulateColumn as fdapc on fdap.FieldDefAutoPopulateID = fdapc.FieldDefAutoPopulateID
inner join tbl_pguidef as pc on fdc.FieldDefID = pc.FieldDefID
inner join tbl_DomainListTable as dltp on fdp.DomainListID = dltp.domainlistid
inner join tbl_DomainListTable as dltp2 on fdp.DomainListID = dltp2.domainlistid
inner join tbl_TableDef as tdpb on dltp.TableDefCode = tdpb.Code and tdpb.TableTypeID = 1
inner join tbl_TableDef as tdpp on dltp2.TableDefCode = tdpp.Code and tdpp.TableTypeID = 2
inner join tbl_ColumnDef as cdpb on tdpb.Code = cdpb.TableDefCode and cdpb.IsPrimaryKey = 1
where fdh.CrossReferenceTypeID = 4 and pc.pid = 1 and cdpb.Code != 'BCscPackageID'";

    APSLFQueries +=  getAPSLFQueries(apBaseQuery,client,server,1);

    string ddQueries = @"
select distinct 'Select pb.' + cdpb.Code + ' as ParentRMSID,rtrim(pb.code) as ParentRMSCode, ''' + pp.displaytext +''' as Parent,NULL as ChildRMSID,'''' as ChildRMSCode,
'''' as Child, pp.displaytext  as ParentValue,pb.isactive as ParentActive,
'''' as ChildValue,'''' as ChildActive,
''Sublist'' as RelationshipType
from ' + tdpb.code + ' as pb 
inner join ' + tdpp.code + ' as pp on pb.' + cdpb.code + ' = pp. ' + cdpb.code + ' and pp.pid = 1'

from ['Req - Core to Custom$'] as t
inner join tbl_FieldDef as fdp on t.[FieldDef Code] = fdp.Code
inner join tbl_pguidef as pp on fdp.FieldDefID = pp.FieldDefID and pid = 1
inner join tbl_DomainListTable as dltp on fdp.DomainListID = dltp.domainlistid
inner join tbl_DomainListTable as dltp2 on fdp.DomainListID = dltp2.domainlistid
inner join tbl_TableDef as tdpb on dltp.TableDefCode = tdpb.Code and tdpb.TableTypeID = 1
inner join tbl_TableDef as tdpp on dltp2.TableDefCode = tdpp.Code and tdpp.TableTypeID = 2
inner join tbl_ColumnDef as cdpb on tdpb.Code = cdpb.TableDefCode and cdpb.IsPrimaryKey = 1
where Domainlist != 'NULL' and fdp.FieldDefID not in (select Parentfielddefid as fielddefid  from tbl_FieldDefHierarchy 
union all
select ChildFieldDefID as fielddefid  from tbl_FieldDefHierarchy )
and [Include For Client?] = 'Yes' 

union all
select distinct 'Select pb.' + cdpb.Code + ' as ParentRMSID,rtrim(pb.code) as ParentRMSCode, ''' + pp.displaytext +''' as Parent,NULL as ChildRMSID,'''' as ChildRMSCode,
'''' as Child, pp.displaytext  as ParentValue,pb.isactive as ParentActive,
'''' as ChildValue,'''' as ChildActive,
''Sublist'' as RelationshipType
from ' + tdpb.code + ' as pb 
inner join ' + tdpp.code + ' as pp on pb.' + cdpb.code + ' = pp. ' + cdpb.code + ' and pp.pid = 1'

from ['Req - Custom to Custom$'] as t
inner join tbl_FieldDef as fdp on t.[FieldDef Code] = fdp.Code
inner join tbl_pguidef as pp on fdp.FieldDefID = pp.FieldDefID and pid = 1
inner join tbl_DomainListTable as dltp on fdp.DomainListID = dltp.domainlistid
inner join tbl_DomainListTable as dltp2 on fdp.DomainListID = dltp2.domainlistid
inner join tbl_TableDef as tdpb on dltp.TableDefCode = tdpb.Code and tdpb.TableTypeID = 1
inner join tbl_TableDef as tdpp on dltp2.TableDefCode = tdpp.Code and tdpp.TableTypeID = 2
inner join tbl_ColumnDef as cdpb on tdpb.Code = cdpb.TableDefCode and cdpb.IsPrimaryKey = 1
where Domainlist != 'NULL'
and fdp.FieldDefID not in (select Parentfielddefid as fielddefid  from tbl_FieldDefHierarchy 
union all
select ChildFieldDefID as fielddefid  from tbl_FieldDefHierarchy )
and [Include For Client?] = 'Yes'
";

    APSLFQueries += getAPSLFQueries(ddQueries , client, server, 1);


    APSLFMappingExport(APSLFQueries,client,server,filePath);
    //emails
    // temp data location versus email pull
    // need to update sql to use prod email and update queries to use server info
   // string getOrgID = @"select * from swa_Security.dbo.organization where code = '" + client + "'";
    //connect to prod or logship email db
    string emailQuery = @"select distinct mc.CandidateID,rfcp.CandProfileID,Subject,SentOn,Replace(Replace(BodyText, CHAR(13), ''), CHAR(10), '') as bodytext,
Replace(Replace(BodyHTML, CHAR(13), ''), CHAR(10), '') as bodyhtml
from message as m with (nolock)
inner join MessageCandidate as mc with (nolock) on m.MessageID = mc.MessageID
inner join MessageRequisition as mr with (nolock) on m.MessageID = mr.MessageID
inner join " + client.Replace("_Export","") + @".dbo.tbl_ReqFolderCandProf as rfcp with (nolock) on mr.RequisitionID = rfcp.ReqID
inner join " + client.Replace("_Export","") + @".dbo.tbl_CandProfile as cp with (nolock) on rfcp.CandProfileID = cp.CandProfileID and mc.CandidateID = cp.CandID 
";

    if (obfuscateFlag == "Y")
    {
        emailQuery = @"select distinct mc.CandidateID,rfcp.CandProfileID, 'Test Subject ' + cast(m.messageid as nvarchar(10)) as Subject,SentOn,'SAMPLE EMAIL TEXT' as bodytext,
'SAMPLE BODY HTML' as bodyhtml
from message as m with (nolock)
inner join MessageCandidate as mc with (nolock) on m.MessageID = mc.MessageID
inner join MessageRequisition as mr with (nolock) on m.MessageID = mr.MessageID
inner join " + client.Replace("_Export", "") + @".dbo.tbl_ReqFolderCandProf as rfcp with (nolock) on mr.RequisitionID = rfcp.ReqID
inner join " + client.Replace("_Export", "") + @".dbo.tbl_CandProfile as cp with (nolock) on rfcp.CandProfileID = cp.CandProfileID and mc.CandidateID = cp.CandID 
";

    }
    

    

    if (filterFlag == "Y")
    {

        emailQuery += @" inner join dba_db.dbo.tempcandprofilelist as tcpl on cp.candprofileid = tcpl.candprofileid ";
    }

    
    //get query to pull no reporting db fields or are they std list?
  

    string conditionalFieldsReport = @"select distinct pg.displaytext ,  case isnull(fields.fielddefid,0)  when 0 then 'NO'  else 'YES' End as [conditional field?] 

from  " + client + @".dbo.tbl_FieldDef as fd with (nolock) 
            inner join " + client + @".dbo.tbl_PGuidef as pg with (nolock) on fd.FielddefID = pg.fielddefID and pg.pid = 1
            inner join " + client + @".dbo.tbl_DBDataType as dbt with (nolock) on fd.dbdatatypeid = dbt.dbdatatypeid
            inner join " + client + @".dbo.tbl_GUIDef as gd with (nolock) on fd.FieldDefID = gd.FieldDefID

            inner join(select parentfielddefid as fielddefid  from " + client + @".dbo.tbl_FieldDefHierarchy with (nolock)
			union select ChildFieldDefID as fielddefid  from " + client + @".dbo.tbl_FieldDefHierarchy ) as fields  
			on fd.FieldDefID = fields.fielddefid
        where  fd.DataArea in ('REQ_CORE') and pg.DisplayText not like 'Background Check%'

		union all
		select distinct pg.displaytext ,  case t.Domainlist  when 'NULL' then 'NO'  else 'YES' End as [conditional field?] 
		from " + client + @".dbo.tbl_FieldDef as f
		inner join " + client + @".dbo.tbl_pguidef as pg on f.fielddefid = pg.fielddefid and pid = 1
		inner join " + client + @".dbo.['Req - Core to Custom$']  as t on f.code = t.[FieldDef Code]
		where [Include For Client?] = 'Yes' and Domainlist != 'NULL'
        and f.fielddefid not in (select ChildFieldDefID as fielddefid  from " + client + @".dbo.tbl_FieldDefHierarchy )
        and f.fielddefid not in (select ParentFieldDefID as fielddefid  from " + client + @".dbo.tbl_FieldDefHierarchy )
        union all
		select distinct pg.displaytext ,  case t.Domainlist  when 'NULL' then 'NO'  else 'YES' End as [conditional field?] 
		from " + client + @".dbo.tbl_FieldDef as f
		inner join " + client + @".dbo.tbl_pguidef as pg on f.fielddefid = pg.fielddefid and pid = 1
		inner join " + client + @".dbo.['Req - Core to Custom$']  as t on f.code = t.[FieldDef Code]
		where [Include For Client?] = 'Yes' and Domainlist = 'NULL'
        and f.fielddefid not in (select ChildFieldDefID as fielddefid  from " + client + @".dbo.tbl_FieldDefHierarchy )
        and f.fielddefid not in (select ParentFieldDefID as fielddefid  from " + client + @".dbo.tbl_FieldDefHierarchy )

		union all
		select distinct pg.displaytext ,  case t.Domainlist  when 'NULL' then 'NO'  else 'YES' End as [conditional field?] 
		from " + client + @".dbo.tbl_FieldDef as f
		inner join " + client + @".dbo.tbl_pguidef as pg on f.fielddefid = pg.fielddefid and pid = 1
		inner join " + client + @".dbo.['Req - Custom to Custom$']  as t on f.code = t.[FieldDef Code]
		where [Include For Client?] = 'Yes' 
        and f.fielddefid not in (select ChildFieldDefID as fielddefid  from " + client + @".dbo.tbl_FieldDefHierarchy )
        and f.fielddefid not in (select ParentFieldDefID as fielddefid  from " + client + @".dbo.tbl_FieldDefHierarchy )";


   conditionalfieldsExport(conditionalFieldsReport,client,server,filePath);
    whereClause = @"where ruf.IsPrimary =1 and re.ReqID <> 1 and  rrt.roletypeid in (3,4) --and reqid = 35
                    and re.reqid not in (select ruf1.reqid
                    from tbl_ReqUserFunction as ruf1 with (nolock)
                    inner join tbl_ReqUserFunction as ruf2 with (nolock) on ruf1.reqid = ruf2.reqid and ruf1.userid = ruf2.userid
                    where ruf1.reqid <> 1 and ruf1.roletypeid = 3 and ruf2.roletypeid = 4 and ruf1.isprimary = 1 and ruf2.isprimary = 1)
                    ";
    string reqUserRoleReport = @"select distinct rtrim(re.ClientReqID) as ClientReqID,u.Email,r.DisplayText as role,prt.DisplayText as type 
                    from tbl_ReqUserFunction as ruf with (nolock)
                    inner join tbl_Requisition as re with (nolock) on ruf.ReqID = re.ReqID
                    inner join tbl_user as u with (nolock) on ruf.UserID = u.userid
                    inner join tbl_pRole as r with (nolock) on u.RoleID = r.RoleID and r.pid = 1
                    inner join tbl_RoleRoleType as rrt with (nolock) on r.RoleID = rrt.RoleID and ruf.roletypeid = rrt.roletypeid
                    inner join tbl_PRoleType as prt with (nolock) on rrt.RoleTypeID = prt.RoleTypeID and prt.pid =1
                     
                    ";

    if (filterFlag == "Y")
    {

        reqUserRoleReport += @"inner join tempreqlist as trl on re.reqid = trl.reqid 
                               ";

    }

    reqUserRoleReport += whereClause;

    whereClause = @" where ruf1.reqid <> 1 and ruf1.roletypeid = 3 and ruf2.roletypeid = 4 and ruf1.isprimary = 1 and ruf2.isprimary = 1 ";              
    string reqUserRolePart2 = @"
                    union all
                    select distinct rtrim(re.clientreqid) as ClientReqID,u.Email,r.DisplayText as role,'Recruiter' as type 
                    from tbl_ReqUserFunction as ruf1 
                    inner join tbl_Requisition as re with (nolock) on ruf1.ReqID = re.ReqID
                    inner join tbl_user as u with (nolock) on ruf1.UserID = u.userid
                    inner join tbl_pRole as r with (nolock) on u.RoleID = r.RoleID and r.pid = 1
                    inner join tbl_ReqUserFunction as ruf2 with (nolock) on ruf1.reqid = ruf2.reqid and ruf1.userid = ruf2.userid
                     ";

    if (filterFlag == "Y")
    {

        reqUserRolePart2 += @"inner join tempreqlist as trl on re.reqid = trl.reqid 
                               ";

    }

    reqUserRoleReport += reqUserRolePart2;
    reqUserRoleReport += whereClause;


    reqUserRoleExport(reqUserRoleReport, client, server,filePath);

    // need import of spreadsheet
    string resultsQuery = "Select canddim.CandID,rtrim(clientreqid) as ClientReqID,";

    string customCandFreeTextFields = @"select distinct fd.code as fieldcode, pg.displaytext, fd.FieldName,dbt.code,so.name
from sys.columns as sc
inner join sys.tables as so with (nolock) on sc.object_id = so.object_id
inner join ['Cand - Custom to Custom$'] as ctc with (nolock) on sc.name = ctc.FieldName and so.name = ctc.TableName
inner join dbo.tbl_FieldDef as fd with (nolock) on fd.FieldName = sc.name and fd.TableName = so.name
inner join dbo.tbl_PGuidef as pg with (nolock) on fd.FielddefID = pg.fielddefID and pg.pid = 1
inner join dbo.tbl_DBDataType as dbt with (nolock) on fd.dbdatatypeid = dbt.dbdatatypeid
where so.name in ('tbl_candfieldvaluefreetext','tbl_candprofilefieldvaluefreetext') and ctc.[Include for Client?] = 'Yes'";

    resultsQuery = buildCustomQuery(customCandFreeTextFields, resultsQuery,client,server,obfuscateFlag);

    string customCandFieldValueFields = @"select distinct fd.code as fieldcode, pg.displaytext, fd.FieldName,dbt.code,so.name 
from sys.columns as sc with (nolock)
inner join sys.tables as so with (nolock) on sc.object_id = so.object_id
inner join ['Cand - Custom to Custom$'] as ctc with (nolock) on sc.name = ctc.FieldName and so.name = ctc.TableName
inner join dbo.tbl_FieldDef as fd with (nolock) on fd.FieldName = sc.name and fd.TableName = so.name
inner join dbo.tbl_PGuidef as pg with (nolock) on fd.FielddefID = pg.fielddefID and pg.pid = 1
inner join dbo.tbl_DBDataType as dbt with (nolock) on fd.dbdatatypeid = dbt.dbdatatypeid
where so.name in ('tbl_candfieldvalue','tbl_candprofilefieldvalue') and ctc.[Include for Client?] = 'Yes'
and fd.DataArea in ('CAND_CORE','CP_CORE')";

    resultsQuery = buildCustomQuery(customCandFieldValueFields, resultsQuery,client,server, obfuscateFlag);

    string candCoreToCustomFields = @"select distinct fd.code as fieldcode, pg.displaytext, case fd.FieldName WHEN 'SSN' then 'SS' 
WHEN 'WorkEligibilityStatusID' THEN 'WorkEligibilityStatusDisplayText'
WHEN 'PostOfferDisabilityStatusID' THEN 'PostOfferDisabilityStatusDisplayText' WHEN 'SourceAvenueTypeID' THEN 'SourceAvenueTypeDisplayText'
else fd.FieldName end as fieldname,case fd.FieldName  
WHEN 'WorkEligibilityStatusID' THEN 'NVARCHAR'
WHEN 'PostOfferDisabilityStatusID' THEN 'NVARCHAR'
else dbt.code end as code,fd.DataArea,case fd.fieldname WHEN 'WorkEligibilityStatusID' THEN 'tbl_CandidateProfileDim'
WHEN 'PostOfferDisabilityStatusID' THEN 'tbl_CandidateProfileDim' WHEN 'SourceAvenueTypeID' THEN 'tbl_CandidateProfileDim' else so.name end as name
from sys.columns as sc with (nolock)
inner join sys.tables as so with (nolock) on sc.object_id = so.object_id
inner join ['Cand - Core to Custom$'] as ctc with (nolock) on sc.name = ctc.FieldName and so.name = ctc.TableName
inner join dbo.tbl_FieldDef as fd with (nolock) on fd.FieldName = sc.name and fd.TableName = so.name
inner join dbo.tbl_PGuidef as pg with (nolock) on fd.FielddefID = pg.fielddefID and pg.pid = 1
inner join dbo.tbl_DBDataType as dbt with (nolock) on fd.dbdatatypeid = dbt.dbdatatypeid
where so.name in ('tbl_candidate','tbl_candprofile','tbl_CandPostOfferVeteranStatus') and ctc.[Include for Client?] = 'Yes'
and fd.DataArea in ('CAND_CORE','CP_CORE')";

    resultsQuery = buildCustomQuery(candCoreToCustomFields, resultsQuery,client,server, obfuscateFlag);
    
   
    
    string endQuery = @" from tbl_CandCustFieldDim_BU1 as cd with (nolock)
inner join tbl_CandidateProfileDim as candp with (nolock) on cd.CandID = candp.CandID
inner join tbl_CandProfileCustFieldDim_BU1 as cpd with (nolock) on candp.CandProfileID = cpd.CandProfileID
inner join tbl_CandidateDim as canddim with (nolock) on cd.CandID = canddim.CandID
inner join  " + client + @".dbo.tbl_CandProfile as cp with (nolock) on cd.CandID = cp.candid and candp.CandProfileID = cp.CandProfileID
inner join  " + client + @".dbo.tbl_ReqFolderCandProf as rfcp with (nolock)on cp.CandProfileID = rfcp.CandProfileID
inner join  " + client + @".dbo.tbl_CandFieldValueFreetext as cfvft with (nolock) on cd.CandID = cfvft.CandID
inner join  " + client + @".dbo.tbl_CandProfilefieldvaluefreetext as cpfvft with (nolock) on cp.CandProfileID = cpfvft.CandProfileID
inner join  " + client + @".dbo.tbl_Requisition as r with (nolock) on rfcp.ReqID = r.ReqID
left outer join " + client + @".dbo.tbl_candpostofferveteranstatus as cpovs with (nolock) on cd.candid = cpovs.candid";

    if (filterFlag == "Y")
    {

        endQuery += " inner join " + client + @".dbo.tempcandprofilelist as tcpl on cp.candprofileid = tcpl.candprofileid ";

    }

    resultsQuery = resultsQuery.Remove(resultsQuery.Length - 1,1);

    resultsQuery += endQuery;

    exportCandCustomFields(resultsQuery,client,server,filePath);


    string beginReqQuery = @"Select rtrim(r.ClientReqID) as ClientReqID, ";
    string reqCustomFreeTextFieldsQuery = @"select distinct fd.code as fieldcode, pg.displaytext, fd.FieldName,dbt.code,so.name 
from sys.columns as sc with (nolock)
inner join sys.tables as so with (nolock) on sc.object_id = so.object_id
inner join ['Req - Custom to Custom$'] as ctc with (nolock)on sc.name = ctc.FieldName and so.name = ctc.TableName
inner join dbo.tbl_FieldDef as fd with (nolock) on fd.FieldName = sc.name and fd.TableName = so.name
inner join dbo.tbl_PGuidef as pg with (nolock) on fd.FielddefID = pg.fielddefID and pg.pid = 1
inner join dbo.tbl_DBDataType as dbt with (nolock) on fd.dbdatatypeid = dbt.dbdatatypeid
where so.name in ('tbl_reqfieldvaluefreetext') and ctc.[Include for Client?] = 'Yes'";

    resultsQuery = "";
    resultsQuery += beginReqQuery;

    resultsQuery = buildCustomQuery(reqCustomFreeTextFieldsQuery, resultsQuery,client,server, obfuscateFlag); 

    string reqCustomFieldValueFieldsQuery = @"select distinct fd.code as fieldcode, pg.displaytext, fd.FieldName,dbt.code ,so.name
from sys.columns as sc with (nolock)
inner join sys.tables as so with (nolock) on sc.object_id = so.object_id
inner join ['Req - Custom to Custom$'] as ctc with (nolock) on sc.name = ctc.FieldName and so.name = ctc.TableName
inner join dbo.tbl_FieldDef as fd with (nolock) on fd.FieldName = sc.name and fd.TableName = so.name
inner join dbo.tbl_PGuidef as pg with (nolock) on fd.FielddefID = pg.fielddefID and pg.pid = 1
inner join dbo.tbl_DBDataType as dbt with (nolock) on fd.dbdatatypeid = dbt.dbdatatypeid
where so.name in ('tbl_reqfieldvalue') and ctc.[Include for Client?] = 'Yes'
and fd.DataArea in ('REQ_CORE')
";

    resultsQuery = buildCustomQuery(reqCustomFieldValueFieldsQuery, resultsQuery,client,server, obfuscateFlag);

    string reqCoreToCustomFieldsQuery = @"select distinct fd.code as fieldcode, pg.displaytext, case fd.FieldName when 'FLSATypeID'
then 'FLSA' WHEN 'EEOCategoryTypeID' THEN 'EEOCategoryTypeDisplayText' WHEN 'SalaryCurrencyTypeID'
THEN 'SalaryCurrency' WHEN 'EducationTypeID' THEN 'MinDegreeDesired' when 'ExperienceTypeID' then 'MinExperienceDesired'
WHEN 'PositionShiftTypeID' THEN 'PositionShiftDisplayText' WHEN 'PositionOpeningTypeID' THEN 'PositionOpeningTypeDisplayText'
WHEN 'LocationID' THEN 'JobLocation' else fd.FieldName end as FieldName,
dbt.code ,fd.DataArea,case fd.FieldName WHEN 'EssentialFunctions' THEN 'tbl_RequisitionBase'
WHEN 'WorkEnvironment' THEN 'tbl_RequisitionBase' When 'Description' THEN 'tbl_ReqDescription' 
WHEN 'FLSATypeID'
then 'tbl_RequistionDim' WHEN 'EEOCategoryTypeID' THEN 'tbl_RequistionDim' WHEN 'SalaryCurrencyTypeID'
THEN 'tbl_RequistionDim' WHEN 'EducationTypeID' THEN 'tbl_RequistionDim' when 'ExperienceTypeID' then 'tbl_RequistionDim'
WHEN 'PositionShiftTypeID' THEN 'tbl_RequistionDim' WHEN 'PositionOpeningTypeID' THEN 'tbl_RequistionDim'
WHEN 'LocationID' THEN 'tbl_RequistionDim'
else so.name end as name
from sys.columns as sc with(nolock)
inner join sys.tables as so with(nolock) on sc.object_id = so.object_id
inner join['Req - Core to Custom$'] as ctc with(nolock) on sc.name = ctc.FieldName and so.name = ctc.TableName
inner join dbo.tbl_FieldDef as fd with(nolock) on fd.FieldName = sc.name and fd.TableName = so.name
inner join dbo.tbl_PGuidef as pg with(nolock) on fd.FielddefID = pg.fielddefID and pg.pid = 1
inner join dbo.tbl_DBDataType as dbt with(nolock) on fd.dbdatatypeid = dbt.dbdatatypeid
where so.name in ('tbl_requisition') and ctc.[Include for Client?] = 'Yes'
and fd.DataArea in ('REQ_CORE')";
    


    resultsQuery = buildCustomQuery(reqCoreToCustomFieldsQuery, resultsQuery,client,server, obfuscateFlag);

    endQuery = @" from tbl_ReqCustfieldDim_BU1 as rcf with (nolock)
inner join tbl_RequisitionDim as rd with (nolock)  on rcf.ReqID = rd.ReqID
inner join tbl_ReqDescriptionDim as rdd with (nolock) on rd.ReqID = rdd.ReqID
inner join " + client + @".dbo.tbl_Requisition as r with (nolock) on rcf.reqid = r.ReqID
inner join " + client + @".dbo.tbl_ReqFieldValueFreetext as rft with (nolock) on r.reqid = rft.ReqID
 ";


    if (filterFlag == "Y")
    {

        endQuery += " inner join " + client + @".dbo.tempreqlist as trl on r.reqid = trl.reqid  ";

    }

    resultsQuery = resultsQuery.Remove(resultsQuery.Length - 1, 1);
    resultsQuery += endQuery;

   exportReqCustomFields(resultsQuery,client,server, filePath);


    emailExport(emailQuery, organizationCode,environment,server,filePath);


    if (attachmentFlag == "Y")
    {

        string attachmentQuery = @"if object_id(N'tempattachment',N'U') IS NOT NULL
    DROP TABLE tempattachment

select distinct cp.candid as CANDIDATEID,cp.CandProfileID as CANDPROFILEID,rtrim(req.ClientReqID) as CLIENTREQID,
'RQ0\Common\Attachments\RMS\Client40_MCGHealth\CandAttachments\' + cast(cp.candid as nvarchar(10)) + '\' + cast(cp.candid as nvarchar(10)) +
'_' + cast(t.candattachmentid as nvarchar(10)) 
--'_' + cast(ca.CandAttachmentID as nvarchar(10)) 
+ right(ca.filename,charindex('.',REVERSE(rtrim(Replace(Replace(ca.filename,N'è','e'),N'é','e'))),0)) as filename,ca.FileName as UIFilename,
case  When r.resumeid > 0 then 'Resume' else 'MISC' end as AttachmentType
into tempattachment
                                    from tbl_candprofile as cp 
									inner join tbl_resume as r on cp.ResumeID = r.ResumeID
									inner join tbl_candattachment as ca on r.CandAttachmentID = ca.CandAttachmentID
									inner join tbl_AttachmentType as aty on ca.AttachmentTypeID = aty.AttachmentTypeID
									inner join tbl_ReqFolderCandProf as rfcp on  cp.CandProfileID = rfcp.CandProfileID
									inner join tbl_Requisition as req on rfcp.reqid = req.reqid
									inner join tempreqmapped as t on cp.CandID = t.candid and ca.CandAttachmentID = t.candattachmentid

									--inner join dba_db.dbo.tempopencandproflist as t on cp.CandProfileID = t.candprofileid
-- grab resumes that are not attachments 
--union all
insert into tempattachment(candidateid,candprofileid,clientreqid,filename,uifilename,attachmenttype)
select distinct cp.candid as CANDIDATEID,cp.CandProfileID as CANDPROFILEID,rtrim(req.ClientReqID) as CLIENTREQID,
'RQ0\Common\Resumes\Client40_MCGHealth\' + cast(right(cp.candid,2) as nvarchar(10)) + '\'+ cast(cp.candid as nvarchar(10)) + '_' + cast(r.ResumeID as nvarchar(10)) + '.htm' as filename,
isnull(ca.FileName,cast(cp.candid as nvarchar(10)) + '_' + cast(r.ResumeID as nvarchar(10)) + '.htm') as UIFilename--+ right(ca.filename,charindex('.',REVERSE(rtrim(Replace(Replace(ca.filename,N'è','e'),N'é','e'))),0)) as filename
,case  When r.resumeid > 0 then 'Resume' else 'MISC' end as AttachmentType
--into tempattachmenth2
                                    from tbl_candprofile as cp 
									inner join tbl_resume as r on cp.ResumeID = r.ResumeID
									left outer join tbl_candattachment as ca on r.CandAttachmentID = ca.CandAttachmentID
									left outer join tbl_AttachmentType as aty on ca.AttachmentTypeID = aty.AttachmentTypeID
									inner join tbl_ReqFolderCandProf as rfcp on cp.CandProfileID = rfcp.CandProfileID
									inner join tbl_Requisition as req on rfcp.reqid = req.reqid
									inner join tempcandprofilelist as t on cp.CandProfileID = t.CandprofileID--and ca.CandAttachmentID = t.candattachmentid
									where ca.AttachmentTypeID is null

 --grab mapped non resume
insert into tempattachment(candidateid,candprofileid,clientreqid,filename,uifilename,attachmenttype)
select distinct cp.candid as CANDIDATEID,cp.CandProfileID as CANDPROFILEID,rtrim(req.ClientReqID) as CLIENTREQID,'RQ0\Common\Attachments\RMS\Client40_MCGHealth\CandAttachments\' + cast(cp.candid as nvarchar(10)) + '\' + cast(cp.candid as nvarchar(10)) 
+ '_' + cast(tr.candattachmentid as nvarchar(10)) 
--+ '_' + cast(ca.CandAttachmentID as nvarchar(10)) 
+ right(ca.filename,charindex('.',REVERSE(rtrim(Replace(Replace(ca.filename,N'è','e'),N'é','e'))),0)) as filename,ca.FileName as UIFilename
, 'MISC' as AttachmentType
--into tempattachment3
                                    
                                    from tbl_candprofile as cp 
									--inner join tbl_resume as r on cp.ResumeID = r.ResumeID
									inner join tbl_candattachment as ca on cp.candid = ca.CandID
									inner join tbl_AttachmentType as aty on ca.AttachmentTypeID = aty.AttachmentTypeID
									inner join tbl_ReqFolderCandProf as rfcp on  cp.CandProfileID = rfcp.CandProfileID
									inner join tbl_Requisition as req on rfcp.reqid = req.reqid
									inner join tempcandprofilelist as t on cp.CandProfileID = t.candprofileid
									inner join tempreqmapped as tr on cp.CandID = t.candid and ca.CandAttachmentID = tr.candattachmentid

									where ca.CandAttachmentID not in (Select CandAttachmentID from tbl_Resume where CandAttachmentID is not null)

--union all
insert into tempattachment(candidateid,candprofileid,clientreqid,filename,uifilename,attachmenttype)
select distinct cp.candid as CANDIDATEID,cp.CandProfileID as CANDPROFILEID,rtrim(req.ClientReqID) as CLIENTREQID,'RQ0\Common\Attachments\RMS\Client40_MCGHealth\CandAttachments\' + cast(cp.candid as nvarchar(10)) + '\' + cast(cp.candid as nvarchar(10)) 
+ '_' + cast(th.mappedCandAttachmentID as nvarchar(10)) 

--+ '_' + cast(va.selectedcandattachmentid as nvarchar(10)) 
+ right(ca.filename,charindex('.',REVERSE(rtrim(Replace(Replace(ca.filename,N'è','e'),N'é','e'))),0)) as filename,ca.FileName as UIFilename
, 'MISC' as AttachmentType
--into tempattachment6b
                                    from tbl_candprofile as cp 
									--inner join tbl_resume as r on cp.ResumeID = r.ResumeID
									inner join tbl_candattachment as ca on cp.candid = ca.CandID
									inner join tbl_AttachmentType as aty on ca.AttachmentTypeID = aty.AttachmentTypeID
									inner join tbl_ReqFolderCandProf as rfcp on  cp.CandProfileID = rfcp.CandProfileID
									inner join tbl_Requisition as req on rfcp.reqid = req.reqid
									inner join tempcandprofilelist as t on cp.CandProfileID = t.candprofileid
									inner join candAttachment_md5hash as th on cp.CandID = th.candid and ca.CandAttachmentID = th.candattachmentid 
									where th.mappedCandAttachmentID not in (Select CandAttachmentID from tempreqmapped )";


        attachmentExport(attachmentQuery, client, server,obfuscateFlag,filePath);

    }
    

    //Console.ReadLine();


}
catch (Exception ex)
{
    Console.WriteLine(ex.ToString());
}



static string getAPSLFQueries(string query, string client,string server,int pass)
{
    SqlConnectionStringBuilder sqlconnstr = new SqlConnectionStringBuilder();
    sqlconnstr.DataSource = server;
    sqlconnstr.InitialCatalog = client;
    sqlconnstr.IntegratedSecurity = true;

    string queryResult = "";
    SqlConnection sqlconn = new SqlConnection(sqlconnstr.ConnectionString);
    sqlconn.Open();
    SqlCommand sqlcmd = sqlconn.CreateCommand();
    sqlcmd.CommandText = query;

    SqlDataReader apslfReader = sqlcmd.ExecuteReader();

    while(apslfReader.Read())
    {
        if (pass > 0)
        {
            queryResult += @"UNION ALL 
                                    ";
        }
        else
        {
            pass = 1;
        }
        queryResult += apslfReader[0].ToString();

    }


   

    return queryResult;

}


static async void buildTempData(string client,string filterTypes,string filterValues,string server,string environment)
{

    List<string> filterTypeList = new List<string>();
    filterTypeList = filterTypes.Split(',').ToList();

    List<string> filterValueList = new List<string>();
    filterValueList = filterValues.Split(',').ToList();

    string filterToAdd = "";

    for (int i = 0; i < filterTypeList.Count(); i++)
    {
       // Console.WriteLine(filterTypeList[i].ToString());
       // Console.WriteLine(filterValueList[i].ToString());
        if (filterTypeList[i].ToString() == "ReqStatus")
        {
            //lookup status based on status code
            filterToAdd += " and reqstatusid in " + filterValueList[i].ToString().Replace(";",",");

        }
        else if (filterTypeList[i].ToString() == "ReqDateAFTER")
        {

            filterToAdd += @" or r.ReqID in (select r.ReqID from tbl_Requisition as r inner join (select reqid,max(CreatedOn) as maxstatusdate from tbl_reqlog where LogEntryTypeID = 1
group by reqid
having max(CreatedOn) > '" + filterValueList[i].ToString() + @"') as trl on r.ReqID = trl.ReqID
where r.reqid <> 1 )";

        }
    }    //create temp tables indexed on reqid, candprofileid
        string buildTempReqTable = @"DROP TABLE IF EXISTS tempreqlist 
                                    Create Table tempreqlist ( ReqID int not null identity(1,1) primary key )";
        string buildTempCandProfileTable = @" DROP TABLE IF EXISTS tempcandprofilelist
                                            Create Table tempcandprofilelist ( CandprofileID int not null  primary key, 
                                                CandID int not null )";
        SqlConnectionStringBuilder sqlconnstr = new SqlConnectionStringBuilder();
        sqlconnstr.DataSource = server;
        sqlconnstr.InitialCatalog = client;
        sqlconnstr.IntegratedSecurity = true;

        string queryResult = "";
        SqlConnection sqlconn = new SqlConnection(sqlconnstr.ConnectionString);
        sqlconn.Open();
        SqlCommand sqlcmd = sqlconn.CreateCommand();
        sqlcmd.CommandText = buildTempReqTable;
        sqlcmd.ExecuteNonQuery();

        sqlcmd.CommandText = buildTempCandProfileTable;
        sqlcmd.ExecuteNonQuery();

        sqlcmd.CommandText = @"set identity_insert tempreqlist on 
                                insert into tempreqlist(reqid) select r.reqid
                                from tbl_Requisition as r with (nolock)
                            where r.reqid <> 1
                            ";
        
        sqlcmd.CommandText += filterToAdd + @"
                set identity_insert tempreqlist off ";

        sqlcmd.ExecuteNonQuery();

        sqlcmd.CommandText = @"  
                        insert into tempcandprofilelist(candid,candprofileid) 
                        select distinct cp.candid,cp.CandProfileID 
from tbl_candidate as c with (nolock)
inner join tbl_candprofile as cp with (nolock) on c.CandID = cp.candid
inner join tbl_ReqFolderCandProf as rfcp with (nolock) on cp.CandProfileID = rfcp.CandProfileID
inner join tbl_Requisition as r with (nolock) on rfcp.ReqID = r.ReqID 
inner join tempreqlist as tr on r.reqid = tr.reqid 

";

        sqlcmd.ExecuteNonQuery();




    sqlcmd.CommandText = @"select candprofileid,candid from tempcandprofilelist "; 

    SqlConnectionStringBuilder emailconnstr = new SqlConnectionStringBuilder();
    emailconnstr.InitialCatalog = "dba_db";
    if (environment == "LON")
    {
        emailconnstr.DataSource = @"LONENGSQL101\LON";
    }
    else
    {
        emailconnstr.DataSource = @"RDCENGSQL101\ATL";

    }
    emailconnstr.IntegratedSecurity = true;

    SqlConnection emailConn = new SqlConnection(emailconnstr.ConnectionString);
    emailConn.Open();
    SqlCommand emailCmd = emailConn.CreateCommand();
    emailCmd.CommandText = buildTempCandProfileTable;
    emailCmd.ExecuteNonQuery();

    DataTable profileTable = new DataTable();

    using (SqlDataAdapter emailAdapter = new SqlDataAdapter(sqlcmd.CommandText,sqlconn))
    {
        emailAdapter.Fill(profileTable);

    }

    using (SqlBulkCopy emailBulkCopy = new SqlBulkCopy(emailConn))
    {
        emailBulkCopy.DestinationTableName = @"tempcandprofilelist";
        emailBulkCopy.WriteToServer(profileTable);
    }
    emailConn.Close();
    sqlconn.Close();
    



}

static async void exportReqCustomFields(string query,string client,string server,string filePath)
{
    var conf = new CsvHelper.Configuration.CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
    {


        ShouldQuote = args =>
        {
            if (string.IsNullOrEmpty(args.Field)) return false;

            return true;

        }


    };
    //need report db and server
    conf.Delimiter = "|";
    conf.Escape = char.Parse("\\");
    const string HTML_TAG_PATTERN = "<.*?>";
    const string HTML_CHAR_PATTERN = "&.*?;";
    TextWriter textWriter = File.CreateText(filePath + @"custom_fields_req.csv");
    var reqCustomCSV = new CsvWriter(textWriter, conf);

    List<string> keys = new List<string>();

    SqlConnectionStringBuilder getReqCustomFields = new SqlConnectionStringBuilder();
    getReqCustomFields.InitialCatalog = client + @"_Report";
    getReqCustomFields.DataSource = server;
    getReqCustomFields.IntegratedSecurity = true;


    SqlConnection getReqCustomFieldsConn = new SqlConnection(getReqCustomFields.ConnectionString);
    getReqCustomFieldsConn.Open();

    SqlCommand getReqCustomFieldsCmd = getReqCustomFieldsConn.CreateCommand();
    getReqCustomFieldsCmd.CommandText = query;

    SqlDataReader getReqCustomFieldsRdr = getReqCustomFieldsCmd.ExecuteReader();
    int x = 1;

    while (getReqCustomFieldsRdr.Read())
    {
        if (x == 1)
        {
            for (int i = 0; i < getReqCustomFieldsRdr.FieldCount; i++)
            {

                keys.Add(getReqCustomFieldsRdr.GetName(i));
            }

            foreach (var headers in keys)
            {
                reqCustomCSV.WriteField(headers);

            }
        }

        reqCustomCSV.NextRecord();
        List<string> values = new List<string>();
        for (int i = 0; i < getReqCustomFieldsRdr.FieldCount; i++)
        {
           // Console.WriteLine(getReqCustomFieldsRdr[i].GetType());
            
            if (getReqCustomFieldsRdr[i].GetType().ToString().ToLower() == "system.string")
            {

                values.Add(Regex.Replace(Regex.Replace(getReqCustomFieldsRdr[i].ToString(), HTML_TAG_PATTERN, string.Empty), HTML_CHAR_PATTERN, string.Empty));
                //values.Add(getReqCustomFieldsRdr[i].ToString());

            }
            else
            if (getReqCustomFieldsRdr[i].GetType().ToString().ToLower() == "system.datetime")
            {
                if (!string.IsNullOrEmpty(getReqCustomFieldsRdr[i].ToString()))
                {
                    values.Add(DateTime.Parse(getReqCustomFieldsRdr[i].ToString()).ToString("yyyy-MM-dd hh:mm:ss tt"));
                }
                else
                {
                    values.Add(getReqCustomFieldsRdr[i].ToString());
                }
            }
            else values.Add(getReqCustomFieldsRdr[i].ToString());
        }
        foreach (var value in values)
        {

            reqCustomCSV.WriteField(value);

        }
       // reqCustomCSV.NextRecord();
        x++;
    }

    getReqCustomFieldsRdr.Close();
    textWriter.Close();



}

static async void exportCandCustomFields(string query,string client,string server,string filePath)
{

    var conf = new CsvHelper.Configuration.CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
    {


        ShouldQuote = args =>
        {
            if (string.IsNullOrEmpty(args.Field)) return false;

            return true;

        }


    };

    conf.Delimiter = "|";
    conf.Escape = char.Parse("\\");


    TextWriter textWriter = File.CreateText(filePath + @"custom_fields_cand.csv");
    var candCustomCSV = new CsvWriter(textWriter,conf);

    List<string> keys = new List<string>();
    //need report server!!!!!
    SqlConnectionStringBuilder getCandCustomFields = new SqlConnectionStringBuilder();
    getCandCustomFields.InitialCatalog = client + @"_Report";
    getCandCustomFields.DataSource = server;
    getCandCustomFields.IntegratedSecurity = true;


    SqlConnection getCandCustomFieldsConn = new SqlConnection(getCandCustomFields.ConnectionString);
    getCandCustomFieldsConn.Open();

    SqlCommand getCandCustomFieldsCmd = getCandCustomFieldsConn.CreateCommand();
    getCandCustomFieldsCmd.CommandText = query;
    getCandCustomFieldsCmd.CommandTimeout = 0;

    SqlDataReader getCandCustomFieldsRdr = getCandCustomFieldsCmd.ExecuteReader();
    int x = 1;

    while (getCandCustomFieldsRdr.Read())
    {
        if (x == 1)
        {
            for (int i = 0; i < getCandCustomFieldsRdr.FieldCount; i++)
            {

                keys.Add(getCandCustomFieldsRdr.GetName(i));
            }

            foreach (var headers in keys)
            {
                candCustomCSV.WriteField(headers);

            }
        }

        candCustomCSV.NextRecord();
        List<string> values = new List<string>();
        for (int i = 0; i < getCandCustomFieldsRdr.FieldCount; i++)
        {

            
            if(getCandCustomFieldsRdr[i].GetType().ToString().ToLower() == "system.datetime")
            {
                if (!string.IsNullOrEmpty(getCandCustomFieldsRdr[i].ToString()))
                {
                    values.Add(DateTime.Parse(getCandCustomFieldsRdr[i].ToString()).ToString("yyyy-MM-dd hh:mm:ss tt"));
                }
                else
                {
                    values.Add(getCandCustomFieldsRdr[i].ToString());

                }
            }
            else
                values.Add(getCandCustomFieldsRdr[i].ToString());

        }
        foreach(var value in values)
        {

            candCustomCSV.WriteField(value);
                       

        }
       // candCustomCSV.NextRecord();
        x++;
    }

    getCandCustomFieldsRdr.Close();
    textWriter.Close();

}

static string buildCustomQuery(string getQuery, string resultsQuery,string client,string server,string obfuscated )
{

    try
    {
        string builtQuery;
        builtQuery = resultsQuery;

        string tableAlias = "";
        SqlConnectionStringBuilder customQueryConnStr = new SqlConnectionStringBuilder();
        customQueryConnStr.InitialCatalog = client;
        customQueryConnStr.DataSource = server;
        customQueryConnStr.IntegratedSecurity = true;

        SqlConnection customCandConn = new SqlConnection(customQueryConnStr.ConnectionString);
        customCandConn.Open();

        SqlCommand customCandCmd = customCandConn.CreateCommand();
        customCandCmd.CommandText = getQuery;

        SqlDataReader customCandDataRdr = customCandCmd.ExecuteReader();

        while (customCandDataRdr.Read())
        {
            string test = customCandDataRdr["name"].ToString();
            if (customCandDataRdr["name"].ToString().ToLower() == "tbl_candfieldvaluefreetext")
            {

                tableAlias = @"cfvft.";
            }
            else
            if (customCandDataRdr["name"].ToString().ToLower() == "tbl_candprofilefieldvaluefreetext")
            {

                tableAlias = @"cpfvft.";
            }
            else if (customCandDataRdr["name"].ToString().ToLower() == "tbl_candprofilefieldvalue")
            {
                tableAlias = @"cpd.";
            }
            else if (customCandDataRdr["name"].ToString().ToLower() == "tbl_candfieldvalue")
            {
                tableAlias = @"cd.";
            }
            else if (customCandDataRdr["name"].ToString().ToLower() == "tbl_candidate")
            {
                tableAlias = @"canddim.";
            }
            else if (customCandDataRdr["name"].ToString().ToLower() == "tbl_candprofile")
            {
                tableAlias = @"cp.";
            }
            else if (customCandDataRdr["name"].ToString().ToLower() == "tbl_CandPostOfferVeteranStatus")
            {
                tableAlias = @"cpovs.";
            }
            else if (customCandDataRdr["name"].ToString().ToLower() == "tbl_requisition")
            {
                tableAlias = @"rd.";
            }
            else if (customCandDataRdr["name"].ToString().ToLower() == "tbl_reqfieldvalue")
            {
                tableAlias = @"rcf.";
            }
            else if (customCandDataRdr["name"].ToString().ToLower() == "tbl_reqfieldvaluefreetext")
            {
                tableAlias = @"rft.";
            }
            else if (customCandDataRdr["name"].ToString().ToLower() == "tbl_requisitionbase")
            {
                tableAlias = @"r.";
            }
            else if (customCandDataRdr["name"].ToString().ToLower() == "tbl_reqdescription")
            {
                tableAlias = @"rdd.";
            }
            else if (customCandDataRdr["name"].ToString().ToLower() == "tbl_CandidateProfileDim")
            {
                tableAlias = @"candp.";
            }
            else if (customCandDataRdr["name"].ToString().ToLower() == "tbl_RequisitionDim")
            {
                tableAlias = @"rd.";
            }

            else
                tableAlias = "";


            if (customCandDataRdr["code"].ToString() != "DATETIME")
            {
                //Console.WriteLine(customCandDataRdr["FieldName"].ToString());
                
                if (customCandDataRdr["FieldName"].ToString().Trim().ToLower() == "disabledtypeid")
                {
                    builtQuery += " cast(" + tableAlias + @"[DisabledTypeDisplayText] as nvarchar(max)) as [" + customCandDataRdr["displaytext"].ToString().Trim() + "],";
                }
                else 
                if (customCandDataRdr["FieldName"].ToString().Trim().ToLower() == "militarystatustypeid")
                {
                    builtQuery += " cast(" + tableAlias + @"[MilitaryStatusDisplayText] as nvarchar(max)) as [" + customCandDataRdr["displaytext"].ToString().Trim() + "],";

                }
                else
                //if (customCandDataRdr["FieldName"].ToString().Trim().ToLower() == "salarycurrencytypeid")
                //{
                //    //FINDME
                //    builtQuery += " cast(" + tableAlias + @"[SalaryCurrencyTypeCode] as nvarchar(max)) as [" + customCandDataRdr["displaytext"].ToString().Trim() + "],";

                //}
                //else

                if (customCandDataRdr["FieldName"].ToString().Trim().ToLower() == "ss")
                {
                    if (obfuscated == "Y")
                    {
                        builtQuery +=  @"NULL as [" + customCandDataRdr["displaytext"].ToString().Trim() + "],";
                    }
                    else
                    {
                        builtQuery += " cast(" + tableAlias + @"[" + customCandDataRdr["FieldName"].ToString().Trim() + "] as nvarchar(max)) as [" + customCandDataRdr["displaytext"].ToString().Trim() + "],";
                    }
                }

                else
                    builtQuery += " cast(" + tableAlias + @"[" + customCandDataRdr["FieldName"].ToString().Trim() + "] as nvarchar(max)) as [" + customCandDataRdr["displaytext"].ToString().Trim() + "],";
                //.Replace("\"", "\"\"")
            }
            else
            {

                builtQuery = builtQuery + " Format([" + tableAlias + @"[" + customCandDataRdr["FieldName"].ToString().Trim() + "],'yyyy-MM-dd hh:mm:ss tt','en-us') as [" + customCandDataRdr["displaytext"].ToString().Trim() + "],";
            }


        }


        return builtQuery;
    }
    catch(Exception ex)
    {
        Console.WriteLine(ex.ToString());
        
        return ex.ToString();

    }
}

static async void emailExport(string query,string clientCode, string environment,string baseServer, string filePath)
{
    //fix client lookup

    //clientCode = "MCG_HEALTH";
    string organization = "";
    SqlConnectionStringBuilder sqlconnstr = new SqlConnectionStringBuilder();

    if (environment == "LON")
    {
        sqlconnstr.DataSource = @"LONENGSQL101\LON";
    }
    else
    {
        sqlconnstr.DataSource = @"RDCENGSQL101\ATL";
    }
    sqlconnstr.InitialCatalog = @"email";
    sqlconnstr.IntegratedSecurity = true;


    //query.Replace("Client40_MCGHealth_Export", "CLient40_MCGHealth");

    SqlConnection sqlconn = new SqlConnection(sqlconnstr.ConnectionString);
    sqlconn.Open();

    SqlConnection orgConn = new SqlConnection(sqlconn.ConnectionString);
    orgConn.Open();
    SqlCommand orgCmd = orgConn.CreateCommand();
    orgCmd.CommandText = @"Use SWA_Security
                        Select organizationid from organization where code = '" + clientCode + "'";
    SqlDataReader orgRdr = orgCmd.ExecuteReader();
    while (orgRdr.Read())
    {
        organization = orgRdr["organizationid"].ToString();

    }
    orgRdr.Close();
    //clientCode = @"Client40_MCGHealth_Export";
    orgConn.Close();
    //sqlconnstr.InitialCatalog = client;
    //sqlconnstr.IntegratedSecurity = true;
    //sqlconnstr.DataSource = baseServer;
    //sqlconnstr.MultipleActiveResultSets = true;

    //orgConn.ConnectionString = sqlconnstr.ConnectionString;

    //orgConn.Open();

    //SqlCommand emailCmd = sqlconn.CreateCommand();

    //emailCmd.CommandText = @"if object_id(N'tempcandprofilelist',N'U') IS NULL
    //                        CREATE TABLE tempcandprofilelist (candprofileid int)";
    //emailCmd.ExecuteNonQuery();

    //orgCmd.CommandText = @"select candprofileid from tempcandprofilelist ";

    //SqlDataReader candprofRdr = orgCmd.ExecuteReader();
    //while (candprofRdr.Read())
    //{

    //    SqlCommand insertCmd = sqlconn.CreateCommand();
    //    insertCmd.CommandText = @"insert into tempcandprofilelist Select " + candprofRdr["candprofileid"].ToString();
    //    insertCmd.ExecuteNonQuery();
    //}


    SqlCommand sqlcmd = sqlconn.CreateCommand();
    sqlcmd.CommandTimeout = 0;
    string whereClause = @" where OrganizationID = " + organization + @" and CandidateID<> 0 and mr.requisitionid <> 0";
    sqlcmd.CommandText = query + whereClause;
    var emailRecordList = new List<emailRecord>();
    SqlDataReader csvReader = sqlcmd.ExecuteReader();

    while (csvReader.Read())
    {
        var oneEmailRecord = new emailRecord();

        oneEmailRecord.candid = Int32.Parse(csvReader["CandidateID"].ToString());
        oneEmailRecord.candprofileid = Int32.Parse(csvReader["CandProfileID"].ToString());

        oneEmailRecord.subject = csvReader["subject"].ToString();
        if (!string.IsNullOrEmpty(csvReader["SentOn"].ToString()))
        {
            oneEmailRecord.sentOn = DateTime.Parse(csvReader["SentOn"].ToString()).ToString("yyyy-MM-dd hh:mm:ss tt");
        }
        oneEmailRecord.bodyText = csvReader["bodytext"].ToString();
        if (string.IsNullOrEmpty(csvReader["bodyhtml"].ToString())) {

            oneEmailRecord.bodyHTML = " ";
        }
        else
        {

            oneEmailRecord.bodyHTML = csvReader["bodyhtml"].ToString();
        }
        


        emailRecordList.Add(oneEmailRecord);

    }
    var conf = new CsvHelper.Configuration.CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
    {


        ShouldQuote = args =>
        {
            if (string.IsNullOrEmpty(args.Field)) return false;

            return true;

        }


    };

    conf.Delimiter = "|";
    conf.Escape = char.Parse("\\");



    using (TextWriter filest = new StreamWriter(filePath + @"emails.csv", false, System.Text.Encoding.UTF8))

    using (var csv = new CsvWriter(filest, conf))
    {


        csv.WriteHeader<emailRecord>();
        csv.NextRecord();
        csv.WriteRecords(emailRecordList);

    }


}

static async void conditionalfieldsExport(string query,string client,string server, string filePath)
{
    SqlConnectionStringBuilder sqlconnstr = new SqlConnectionStringBuilder();
    sqlconnstr.DataSource = server;
    sqlconnstr.InitialCatalog = client + "_Report";
    sqlconnstr.IntegratedSecurity = true;

    SqlConnection sqlconn = new SqlConnection(sqlconnstr.ConnectionString);
    sqlconn.Open();

    SqlCommand sqlcmd = sqlconn.CreateCommand();
    sqlcmd.CommandTimeout = 0;
    sqlcmd.CommandText = query;

    var conditionFieldsList = new List<condFieldRecord>();
    SqlDataReader csvReader = sqlcmd.ExecuteReader();

    while (csvReader.Read())
    {
        var oneCondFieldRecord = new condFieldRecord();


        oneCondFieldRecord.condField = csvReader["conditional field?"].ToString();
        oneCondFieldRecord.displaytext = csvReader["displaytext"].ToString();

        conditionFieldsList.Add(oneCondFieldRecord);

    }
    var conf = new CsvHelper.Configuration.CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
    {


        ShouldQuote = args =>
        {
            if (string.IsNullOrEmpty(args.Field)) return false;

            return true;

        }


    };

    conf.Delimiter = "|";
    conf.Escape = char.Parse("\\");



    using (TextWriter filest = new StreamWriter(filePath + @"custom_fields.csv", false, System.Text.Encoding.UTF8))

    using (var csv = new CsvWriter(filest, conf))
    {


        csv.WriteHeader<condFieldRecord>();
        csv.NextRecord();
        csv.WriteRecords(conditionFieldsList);

    }


}

static async void reqUserRoleExport(string query,string client,string server,string filePath)
{
    SqlConnectionStringBuilder sqlconnstr = new SqlConnectionStringBuilder();
    sqlconnstr.DataSource = server;
    sqlconnstr.InitialCatalog = client;
    sqlconnstr.IntegratedSecurity = true;

    SqlConnection sqlconn = new SqlConnection(sqlconnstr.ConnectionString);
    sqlconn.Open();

    SqlCommand sqlcmd = sqlconn.CreateCommand();
    sqlcmd.CommandTimeout = 0;
    sqlcmd.CommandText = query;

    var reqUserRoleList = new List<reqUserRoleRecord>();
    SqlDataReader csvReader = sqlcmd.ExecuteReader();

    while (csvReader.Read())
    {
        var oneReqUserRoleRecord = new reqUserRoleRecord();


        oneReqUserRoleRecord.clientReqID = csvReader["ClientReqID"].ToString();
        oneReqUserRoleRecord.email = csvReader["email"].ToString();
        oneReqUserRoleRecord.role = csvReader["role"].ToString();
        oneReqUserRoleRecord.type = csvReader["type"].ToString();

        reqUserRoleList.Add(oneReqUserRoleRecord);

    }
    var conf = new CsvHelper.Configuration.CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
    {


        ShouldQuote = args =>
        {
            if (string.IsNullOrEmpty(args.Field)) return false;

            return true;

        }


    };

    conf.Delimiter = "|";
    conf.Escape = char.Parse("\\");



    using (TextWriter filest = new StreamWriter(filePath + @"requisition_user_role.csv", false, System.Text.Encoding.UTF8))

    using (var csv = new CsvWriter(filest, conf))
    {


        csv.WriteHeader<condFieldRecord>();
        csv.NextRecord();
        csv.WriteRecords(reqUserRoleList);

    }


}

static async void APSLFMappingExport(string query, string client, string server, string filePath)
{
    SqlConnectionStringBuilder sqlconnstr = new SqlConnectionStringBuilder();
    sqlconnstr.DataSource = server;
    sqlconnstr.InitialCatalog = client;
    sqlconnstr.IntegratedSecurity = true;

    SqlConnection sqlconn = new SqlConnection(sqlconnstr.ConnectionString);
    sqlconn.Open();

    SqlCommand sqlcmd = sqlconn.CreateCommand();
    sqlcmd.CommandTimeout = 0;
    sqlcmd.CommandText = query;

    var APSLFRecordList = new List<APSLF>();
    SqlDataReader csvReader = sqlcmd.ExecuteReader();

    while (csvReader.Read())
    {
        var oneAPSLFRecord = new APSLF();


        oneAPSLFRecord.ParentRMSID = csvReader["ParentRMSID"].ToString();
       
        oneAPSLFRecord.ChildRMSID = csvReader["ChildRMSID"].ToString();
  
        oneAPSLFRecord.ParentRMSCode = csvReader["ParentRMSCode"].ToString();
        oneAPSLFRecord.ChildRMSCode = csvReader["ChildRMSCode"].ToString();
        oneAPSLFRecord.ParentValue = csvReader["ParentValue"].ToString();
        oneAPSLFRecord.ParentActive = csvReader["ParentActive"].ToString();
        oneAPSLFRecord.ChildValue = csvReader["ChildValue"].ToString();
        oneAPSLFRecord.ChildActive = csvReader["ChildActive"].ToString();
        oneAPSLFRecord.RelationshipType = csvReader["RelationshipType"].ToString();
        oneAPSLFRecord.Parent = csvReader["Parent"].ToString();
        oneAPSLFRecord.Child = csvReader["Child"].ToString();



        APSLFRecordList.Add(oneAPSLFRecord);

    }
    var conf = new CsvHelper.Configuration.CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
    {


        ShouldQuote = args =>
        {
            if (string.IsNullOrEmpty(args.Field)) return false;

            return true;

        }


    };

    conf.Delimiter = "|";
    conf.Escape = char.Parse("\\");



    using (TextWriter filest = new StreamWriter(filePath + @"conditional_mapping_rule.csv", false, System.Text.Encoding.UTF8))

    using (var csv = new CsvWriter(filest, conf))
    {


        csv.WriteHeader<APSLF>();
        csv.NextRecord();
        csv.WriteRecords(APSLFRecordList);

    }


}




static async void attachmentExport(string query, string client, string server,string obfuscateFlag,string filePath)
{
    SqlConnectionStringBuilder sqlconnstr = new SqlConnectionStringBuilder();
    sqlconnstr.DataSource = server;
    sqlconnstr.InitialCatalog = client;
    sqlconnstr.IntegratedSecurity = true;

    SqlConnection sqlconn = new SqlConnection(sqlconnstr.ConnectionString);
    sqlconn.Open();

    SqlCommand sqlcmd = sqlconn.CreateCommand();
    sqlcmd.CommandTimeout = 0;
    sqlcmd.CommandText = @"select distinct candid,hashvalue into #temphash from candAttachment_md5hash 
                                        group by candid,hashvalue

select m.candid,m.hashvalue,max(candattachmentid) as selectedCandattachmentid into #tempmappedhash from #temphash as t
inner join candAttachment_md5hash as m on t.candid = m.candid and t.hashvalue = m.hashvalue
--where mappedCandAttachmentID is null
group by m.candid,m.hashvalue

update candAttachment_md5hash
set mappedCandAttachmentID = t.selectedCandattachmentid
from candAttachment_MD5Hash as m
inner join #tempmappedhash as t on m.candid = t.candid and m.hashvalue = t.hashvalue

select distinct t.candid,t.mappedCandAttachmentID as candattachmentid,r.ReqID into #tempreqmapped
from candAttachment_md5hash as t
inner join tbl_Candattachment as ca on t.candattachmentid = ca.candattachmentid
inner join tbl_CandProfile as cp on t.candid = cp.CandID
inner join tbl_ReqFolderCandProf as rfcp on cp.CandProfileID = rfcp.CandProfileID
inner join tbl_Requisition as r on rfcp.ReqID = r.ReqID
inner join tempcandprofilelist as tcpl on cp.CandProfileID = tcpl.CandprofileID
and ca.createdon between dateadd(mi,-5,cp.CreatedOn) and dateadd(mi,5,cp.CreatedOn) 

insert into #tempreqmapped(candid,candattachmentid,ReqID)
select distinct t.candid,t.mappedCandAttachmentID,r.ReqID 
from candAttachment_md5hash as t
inner join tbl_Candattachment as ca on t.candattachmentid = ca.candattachmentid
inner join tbl_CandProfile as cp on t.candid = cp.CandID
inner join tbl_ReqFolderCandProf as rfcp on cp.CandProfileID = rfcp.CandProfileID
inner join tbl_Requisition as r on rfcp.ReqID = r.ReqID
inner join tbl_resume as rm on ca.CandAttachmentID = rm.CandAttachmentID
inner join tempcandprofilelist as tcpl on cp.CandProfileID = tcpl.CandprofileID

if object_id(N'tempreqmapped',N'U') IS NOT NULL
    DROP TABLE tempreqmapped

select * into tempreqmapped from #tempreqmapped

";

    sqlcmd.ExecuteNonQuery();








    sqlcmd.CommandText = query;

    sqlcmd.ExecuteNonQuery();


    
    if (obfuscateFlag == "Y")
    {
        sqlcmd.CommandText = @"select CANDIDATEID,CANDPROFILEID,clientreqid,filename,CANDIDATEID as uifilename,attachmenttype from tempattachment";

    }

    else
    {

        sqlcmd.CommandText = @"select CANDIDATEID,CANDPROFILEID,clientreqid,filename,uifilename,attachmenttype from tempattachment";
    }

    var attachmentRecordList = new List<attachment>();
    SqlDataReader csvReader = sqlcmd.ExecuteReader();

    while (csvReader.Read())
    {
        var oneAttachmentRecord = new attachment();


        oneAttachmentRecord.CANDIDATEID = Int32.Parse(csvReader["CANDIDATEID"].ToString());
        oneAttachmentRecord.CANDPROFILEID = Int32.Parse(csvReader["CANDPROFILEID"].ToString());
        oneAttachmentRecord.CLIENTREQID = csvReader["clientreqid"].ToString();
        oneAttachmentRecord.filename = csvReader["filename"].ToString();
        oneAttachmentRecord.UIFilename = csvReader["uifilename"].ToString();
        oneAttachmentRecord.AttachmentType = csvReader["attachmenttype"].ToString();

        attachmentRecordList.Add(oneAttachmentRecord);

    }
    var conf = new CsvHelper.Configuration.CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
    {


        ShouldQuote = args =>
        {
            if (string.IsNullOrEmpty(args.Field)) return false;

            return true;

        }


    };

    conf.Delimiter = "|";
    conf.Escape = char.Parse("\\");



    using (TextWriter filest = new StreamWriter(filePath + @"attachments.csv", false, System.Text.Encoding.UTF8))

    using (var csv = new CsvWriter(filest, conf))
    {


        csv.WriteHeader<attachment>();
        csv.NextRecord();
        csv.WriteRecords(attachmentRecordList);

    }


}


static async void candEmploymentExport(string query,string client,string server,string filePath)
{
    SqlConnectionStringBuilder sqlconnstr = new SqlConnectionStringBuilder();
    sqlconnstr.DataSource = server;
    sqlconnstr.InitialCatalog = client;
    sqlconnstr.IntegratedSecurity = true;

    SqlConnection sqlconn = new SqlConnection(sqlconnstr.ConnectionString);
    sqlconn.Open();

    SqlCommand sqlcmd = sqlconn.CreateCommand();
    sqlcmd.CommandTimeout = 0;
    sqlcmd.CommandText = query;

    var empRecordList = new List<candEmploymentRecord>();
    SqlDataReader csvReader = sqlcmd.ExecuteReader();
    
    while (csvReader.Read())
    {
        var oneEmploymentRecord = new candEmploymentRecord();

        
        oneEmploymentRecord.candid = Int32.Parse(csvReader["CANDID"].ToString());
        oneEmploymentRecord.candprofileid = Int32.Parse(csvReader["CANDPROFILEID"].ToString());
        oneEmploymentRecord.companyName = csvReader["COMPANYNAME"].ToString();
        if (!string.IsNullOrEmpty(csvReader["EMPLOYMENTSTARTDATE"].ToString()))
        {
            oneEmploymentRecord.employmentStartDate = DateTime.Parse(csvReader["EMPLOYMENTSTARTDATE"].ToString()).ToString("yyyy-MM-dd hh:mm:ss tt");
        }
        if (!string.IsNullOrEmpty(csvReader["EMPLOYMENTENDDATE"].ToString()))
        {
            oneEmploymentRecord.employmentEndDate = DateTime.Parse(csvReader["EMPLOYMENTENDDATE"].ToString()).ToString("yyyy-MM-dd hh:mm:ss tt");
        }
        
        empRecordList.Add(oneEmploymentRecord);

    }
    var conf = new CsvHelper.Configuration.CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
    {


        ShouldQuote = args =>
        {
            if (string.IsNullOrEmpty(args.Field)) return false;

            return true;

        }


    };

    conf.Delimiter = "|";
    conf.Escape = char.Parse("\\");



    using (TextWriter filest = new StreamWriter(filePath + @"cand_employment.csv", false, System.Text.Encoding.UTF8))

    using (var csv = new CsvWriter(filest, conf))
    {


        csv.WriteHeader<candEmploymentRecord>();
        csv.NextRecord();
        csv.WriteRecords(empRecordList);

    }


}
static async void candEducationExport(string query,string client,string server,string filePath)
{
    SqlConnectionStringBuilder sqlconnstr = new SqlConnectionStringBuilder();
    sqlconnstr.DataSource = server;
    sqlconnstr.InitialCatalog = client;
    sqlconnstr.IntegratedSecurity = true;

    SqlConnection sqlconn = new SqlConnection(sqlconnstr.ConnectionString);
    sqlconn.Open();

    SqlCommand sqlcmd = sqlconn.CreateCommand();
    sqlcmd.CommandTimeout = 0;
    sqlcmd.CommandText = query;

    var eduRecordList = new List<candEducationRecord>();
    SqlDataReader csvReader = sqlcmd.ExecuteReader();

    while (csvReader.Read())
    {
        var oneEducationRecord = new candEducationRecord();

        oneEducationRecord.candid = Int32.Parse(csvReader["CANDID"].ToString());
        oneEducationRecord.candprofileid = Int32.Parse(csvReader["CANDPROFILEID"].ToString());
        oneEducationRecord.collegeName = csvReader["COLLEGENAME"].ToString();
        oneEducationRecord.collegeMajor = csvReader["COLLEGEMAJOR"].ToString();
        oneEducationRecord.collegeDegree = csvReader["COLLEGEDEGREE"].ToString();

        if (!string.IsNullOrEmpty(csvReader["GRADUATIONDate"].ToString()))
        {
           
            oneEducationRecord.graduationDate = DateTime.Parse(csvReader["GRADUATIONDate"].ToString()).ToString("yyyy-MM-dd hh:mm:ss tt");
        }

        

        eduRecordList.Add(oneEducationRecord);

    }
    var conf = new CsvHelper.Configuration.CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
    {


        ShouldQuote = args =>
        {
            if (string.IsNullOrEmpty(args.Field)) return false;

            return true;

        }


    };

    conf.Delimiter = "|";
    conf.Escape = char.Parse("\\");



    using (TextWriter filest = new StreamWriter(filePath + @"cand_education.csv", false, System.Text.Encoding.UTF8))

    using (var csv = new CsvWriter(filest, conf))
    {


        csv.WriteHeader<candEducationRecord>();
        csv.NextRecord();
        csv.WriteRecords(eduRecordList);

    }


}

static async void reqExport(string query,string client,string server,string filePath)
{

    const string HTML_TAG_PATTERN = "<.*?>";
    const string HTML_CHAR_PATTERN = "&.*?;";

    SqlConnectionStringBuilder sqlconnstr = new SqlConnectionStringBuilder();
    sqlconnstr.DataSource = server;
    sqlconnstr.InitialCatalog = client;
    sqlconnstr.IntegratedSecurity = true;

    SqlConnection sqlconn = new SqlConnection(sqlconnstr.ConnectionString);
    sqlconn.Open();

    SqlCommand sqlcmd = sqlconn.CreateCommand();
    sqlcmd.CommandTimeout = 0;
    sqlcmd.CommandText = query;

    var reqRecordList = new List<reqRecord>();
    SqlDataReader csvReader = sqlcmd.ExecuteReader();
    //var candExportCSV = new CsvExport("|", false, true);
    while (csvReader.Read())
    {
        var OneReqRecord = new reqRecord();

        OneReqRecord.CLIENTREQID = csvReader["CLIENTREQID"].ToString();
        OneReqRecord.CREATEDON = DateTime.Parse(csvReader["CREATEDON"].ToString()).ToString("yyyy-MM-dd hh:mm:ss tt");
        OneReqRecord.CREATE_USER = csvReader["CREATE_USER"].ToString();
        string datatype = csvReader["DESCRIPTION"].GetType().ToString();
        OneReqRecord.DESCRIPTION = Regex.Replace(Regex.Replace(csvReader["DESCRIPTION"].ToString(), HTML_TAG_PATTERN, string.Empty),HTML_CHAR_PATTERN,string.Empty);
        OneReqRecord.EducationType = csvReader["EducationType"].ToString();
        OneReqRecord.RECRUITSTARTDATE = csvReader["RECRUITSTARTDATE"].ToString();
        OneReqRecord.RECRUITENDDATE = csvReader["RECRUITENDDATE"].ToString();
        OneReqRecord.CITY = csvReader["CITY"].ToString();
        OneReqRecord.COUNTRYCODE = csvReader["COUNTRYCODE"].ToString();
        OneReqRecord.LOCATIONDISPLAYTEXT = csvReader["LOCATIONDISPLAYTEXT"].ToString();
        OneReqRecord.STATEPROVINCE = csvReader["LOCATIONDISPLAYTEXT"].ToString();
        OneReqRecord.TITLE = csvReader["TITLE"].ToString();
        OneReqRecord.WORKFLOWDISPLAYTEXT = csvReader["WORKFLOWDISPLAYTEXT"].ToString();
        OneReqRecord.EDUCATIONDISPLAYTEXT = csvReader["EDUCATIONDISPLAYTEXT"].ToString();
        OneReqRecord.MODIFIEDON = DateTime.Parse(csvReader["MODIFIEDON"].ToString()).ToString("yyyy-MM-dd hh:mm:ss tt");
        OneReqRecord.ReqStatus = csvReader["ReqStatus"].ToString();
        OneReqRecord.EDUCATION = csvReader["EDUCATION"].ToString();
        OneReqRecord.EXPIERENCE = csvReader["EXPIERENCE"].ToString();
        OneReqRecord.POSITIONTYPE = csvReader["POSITIONTYPE"].ToString();
        OneReqRecord.POSITIONCATEGORY = csvReader["POSITIONCATEGORY"].ToString();
        OneReqRecord.DEPARTMENT = csvReader["DEPARTMENT"].ToString();
        OneReqRecord.SalaryLow = csvReader["SalaryLow"].ToString();
        OneReqRecord.SalaryHigh = csvReader["SalaryHigh"].ToString();

        reqRecordList.Add(OneReqRecord);

    }
    var conf = new CsvHelper.Configuration.CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
    {


        ShouldQuote = args =>
        {
            if (string.IsNullOrEmpty(args.Field)) return false;

            return true;

        }


    };

    conf.Delimiter = "|";
    conf.Escape = char.Parse("\\");



    using (TextWriter filest = new StreamWriter(filePath + @"requisition.csv", false, System.Text.Encoding.UTF8))

    using (var csv = new CsvWriter(filest, conf))
    {

       
        csv.WriteHeader<reqRecord>();
        csv.NextRecord();
        csv.WriteRecords(reqRecordList);

    }

}

static string getServer(string env, string clientname)
{

    string server = "";
    string commonServer = "";

    if (env == "ATL")
    {

        commonServer = @"ATLINFSQLC01V1\INST1";

    }
    else
    {
        commonServer = @"LONINFSQLC03V1\INST1";

    }

    SqlConnectionStringBuilder sqlconnstr = new SqlConnectionStringBuilder();
    sqlconnstr.InitialCatalog = @"common40";
    sqlconnstr.DataSource = commonServer;
    sqlconnstr.IntegratedSecurity = true;

    SqlConnection sqlconn = new SqlConnection(sqlconnstr.ConnectionString);
    sqlconn.Open();

    SqlCommand sqlcmd = sqlconn.CreateCommand();
    sqlcmd.CommandText = @"select hostname from tbl_client as c inner join tbl_server as s on c.serverid = s.serverid 
            where c.dbname = '" + clientname + "'";
    SqlDataReader commonRdr = sqlcmd.ExecuteReader();

    while (commonRdr.Read())
    {
        server = commonRdr["hostname"].ToString();

    }



    return server;

}

static async void candExport(string query,string client,string server,string filePath)
{
    SqlConnectionStringBuilder sqlconnstr = new SqlConnectionStringBuilder();
    sqlconnstr.DataSource = server;
    sqlconnstr.InitialCatalog = client;
    sqlconnstr.IntegratedSecurity = true;

    SqlConnection sqlconn = new SqlConnection(sqlconnstr.ConnectionString);
    sqlconn.Open();

    SqlCommand sqlcmd = sqlconn.CreateCommand();
    sqlcmd.CommandTimeout = 0;
    sqlcmd.CommandText = query;

    var candRecordList = new List<candRecord>();
    SqlDataReader csvReader = sqlcmd.ExecuteReader();
    //var candExportCSV = new CsvExport("|", false, true);
    while (csvReader.Read())
    {
        candRecord oneCandRecord = new candRecord();

        oneCandRecord.CANDID = Int32.Parse(csvReader["CANDID"].ToString());
        oneCandRecord.CANDPROFILEID = Int32.Parse(csvReader["CANDPROFILEID"].ToString());
        oneCandRecord.ADDRESS1 = csvReader["ADDRESS1"].ToString();
        oneCandRecord.CREATEDON = DateTime.Parse(csvReader["CREATEDON"].ToString()).ToString("yyyy-MM-dd hh:mm:ss tt");
        oneCandRecord.PRIMARYEMAILADDRESS = csvReader["PRIMARYEMAILADDRESS"].ToString();
        oneCandRecord.NAME = csvReader["NAME"].ToString();
        oneCandRecord.CLIENTREQID = csvReader["CLIENTREQID"].ToString();
        oneCandRecord.SOURCEAVENUEVALUE = csvReader["SOURCEAVENUEVALUE"].ToString();
        oneCandRecord.FOLDER = csvReader["FOLDER"].ToString();
        oneCandRecord.FOLDERDISPLAYTEXT = csvReader["FOLDERDISPLAYTEXT"].ToString();
        oneCandRecord.MODIFIEDON = DateTime.Parse(csvReader["MODIFIEDON"].ToString()).ToString("yyyy-MM-dd hh:mm:ss tt");
        oneCandRecord.WORKFLOW = csvReader["WORKFLOW"].ToString();
        oneCandRecord.WORKFLOWDISPLAYTEXT = csvReader["WORKFLOWDISPLAYTEXT"].ToString();
        oneCandRecord.PHONE = csvReader["PHONE"].ToString();
        oneCandRecord.GENDER = csvReader["GENDER"].ToString();
        oneCandRecord.Ethnicity = csvReader["Ethnicity"].ToString();
        oneCandRecord.VeteranStatus = csvReader["VeteranStatus"].ToString();
        oneCandRecord.DISABILITY = csvReader["DISABILITY"].ToString();
        oneCandRecord.source = csvReader["source"].ToString();

        candRecordList.Add(oneCandRecord);


    }
    var conf = new CsvHelper.Configuration.CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture) {


        
        ShouldQuote = args =>
        {
            if (string.IsNullOrEmpty(args.Field)) return false;

            return true;

        }


    };

    conf.Delimiter = "|";
    conf.Escape = char.Parse("\\");
    



    using (TextWriter filest = new StreamWriter(filePath +  @"candidate.csv", false,System.Text.Encoding.UTF8))
    
    using (var csv = new CsvWriter(filest,conf))
    {

        csv.WriteHeader<candRecord>();
        csv.NextRecord();
        csv.WriteRecords(candRecordList);


    }

}


static void GetOrganizationCode(string client, string environment, out string organizationCode)
{
    organizationCode = @"";

    SqlConnectionStringBuilder sqlconnstr = new SqlConnectionStringBuilder();
    sqlconnstr.InitialCatalog = @"SWA_Security";
    if (environment == "ATL")
    {

        sqlconnstr.DataSource = @"ATLINFSQLC01V1\INST1";

    }
    else
    {
        sqlconnstr.DataSource = @"LONINFSQLC03V1\INST1";

    }

    sqlconnstr.IntegratedSecurity = true;
    SqlConnection sqlconn = new SqlConnection(sqlconnstr.ConnectionString);
    sqlconn.Open();

    SqlCommand sqlcmd = sqlconn.CreateCommand();
    sqlcmd.CommandText = @"select o.code from organization as o inner join common40.dbo.tbl_clientbusinessunit as cbu on o.loginkey = cbu.idtag inner join common40.dbo.tbl_client as c on cbu.clientid = c.clientid 
                                where c.dbname = '" + client + "'";

    SqlDataReader sqlDataReader = sqlcmd.ExecuteReader();

    while (sqlDataReader.Read())
    {
        organizationCode = sqlDataReader["code"].ToString();

    }

}

static void GetLatestFullBackup(string ClientName, string BAKLocation, out string prodBakFile, string baktype, string server, string fullFileName)
{
    string file;
    int dotpos;
    string filetype;
    string folderName = "";

    if (baktype == "BAK")
    {
        folderName = "FULL";

    }
    else
    {
        folderName = "DIFF";
    }

    prodBakFile = "";
    string serverPathName = server.Replace(@"\", "$");
    //string filename;

    DateTime datemodified = DateTime.Parse("2001/01/01");
    file = "";
    System.IO.FileInfo fullFile = new FileInfo(BAKLocation + @"\" + serverPathName + @"\" + ClientName + @"\FULL\" + fullFileName);

    if (System.IO.Directory.Exists(BAKLocation + "\\" + serverPathName + "\\" + ClientName + "\\" + folderName))
    {
        string[] files = System.IO.Directory.GetFiles(BAKLocation + "\\" + serverPathName + "\\" + ClientName + "\\" + folderName);

        foreach (string s in files)
        {



            dotpos = s.IndexOf(".");
            filetype = s.Substring(dotpos + 1, 3);
            //filename = s.Substring(1, s.Length - 24);
            System.IO.FileInfo fi = null;
            fi = new System.IO.FileInfo(s);

            if (filetype.ToUpper() == baktype)
            {
                if (baktype == "DFR")
                {
                    if (fi.LastWriteTime > datemodified && fi.LastWriteTime > fullFile.LastWriteTime)
                    {
                        file = fi.Name;
                        datemodified = fi.LastWriteTime;
                    }
                }
                else
                {
                    if (fi.LastWriteTime > datemodified)
                    {
                        file = fi.Name;
                        datemodified = fi.LastWriteTime;
                    }

                }
            }

        }

        prodBakFile = file;

    }
    else
    {
        prodBakFile = "Backup Files do not exist. Contact Copy Tool Support";
    }
}

//restore dbs on etl server
//RestoreFullDatabase(exportServer, ClientName, server, dataCenter);




static void GetReportBackup(string backupPath, out string reportBakFile)
{
    reportBakFile = "";
    string file;
    int dotpos;
    string filetype;


    //string filename;

    DateTime datemodified = DateTime.Parse("2001/01/01");
    file = "";

    if (System.IO.Directory.Exists(backupPath))
    {
        string[] files = System.IO.Directory.GetFiles(backupPath);

        foreach (string s in files)
        {
            System.IO.FileInfo fi = null;
            fi = new System.IO.FileInfo(s);

            reportBakFile = fi.Name;

        }
    }
}

static void GetFileList(string serverName, string bakPath, string ClientName, out string dataFile, out string logFile, string fullFileName, string dbtype)
{



    DataTable dt;
    Restore res = new Restore();

    SqlConnectionStringBuilder sqlconnstr = new SqlConnectionStringBuilder();
    sqlconnstr.DataSource = serverName;
    sqlconnstr.InitialCatalog = @"master";
    sqlconnstr.IntegratedSecurity = true;

    SqlConnection sqlConn = new SqlConnection(sqlconnstr.ConnectionString);


    //string sql;
    //SqlConnection sqlConn = new SqlConnection(connstring.ConnectionString);
    Server srv = new Server(sqlConn.DataSource);
    if (dbtype == "client")
        res.Devices.AddDevice(bakPath + @"\" + serverName.Replace(@"\", "$") + @"\" + ClientName + @"\FULL\" + fullFileName, DeviceType.File);
    else
        res.Devices.AddDevice(bakPath + @"\" + fullFileName, DeviceType.File);

    res.Database = ClientName;
    dt = res.ReadFileList(srv);
    dataFile = dt.Rows[0]["LogicalName"].ToString();
    logFile = dt.Rows[1]["LogicalName"].ToString();



}



static void RestoreFullDatabase(string etlServer, string ClientName, string server, string dataCenter)
{
    Server destinationServer;
    SqlConnectionStringBuilder sqlConnString;
    Restore dbRestore = new Restore();
    
    sqlConnString = new SqlConnectionStringBuilder();
    sqlConnString.InitialCatalog = @"master";
    sqlConnString.DataSource = etlServer;
    sqlConnString.IntegratedSecurity = true;

    SqlConnection sqlConn = new SqlConnection(sqlConnString.ConnectionString);
    //string sourceserver = "";
    sqlConn.Open();
    string exportClientName = ClientName + @"_Export";
    string exportReportName = exportClientName + @"_Report";
    
    destinationServer = new Server(sqlConn.DataSource);

    foreach (Database db in destinationServer.Databases)
    {
        if (db.Name.ToUpper() == exportClientName.ToUpper())
        {
            destinationServer.KillAllProcesses(exportClientName);
            db.Drop();
            break;
        }
        if (db.Name.ToUpper() == exportReportName.ToUpper())
        {
            destinationServer.KillAllProcesses(exportReportName);
            db.Drop();
            break;
        }

    }


    string fullFileName = "";
    string diffFileName = "";
    string reportFullBackName = "";

    dbRestore.Database = exportClientName;
    dbRestore.Action = RestoreActionType.Database;
    dbRestore.PercentCompleteNotification = 1;
    string clientDbBackupPath = @"";
    //\\lonrpxflr01\LONRMSSQLBackup01
    //\\Atlsqlbkup01\ATLRMSBackup01\

    string reportDbBackupPath = @"";
    if (dataCenter == "ATL")
    {
        reportDbBackupPath = @"\\atlsqlbkup02\ATLRMSETLBackup\" + etlServer + @"\" + ClientName + @"_Report\Full\";
        clientDbBackupPath = @"\\Atlsqlbkup01\ATLRMSBackup01";
        GetLatestFullBackup(ClientName, clientDbBackupPath, out fullFileName, "BAK", server, fullFileName);
        GetLatestFullBackup(ClientName, clientDbBackupPath, out diffFileName, "DFR", server, fullFileName);
        GetReportBackup(reportDbBackupPath, out reportFullBackName);
    }
    else
    {
        //fix london path
        string etlServer2 = @"LONINFSQLC03V4$INST4";
        reportDbBackupPath = @"\\lonrpxflr01\LONRMSSQLBackup02\" + etlServer2 + @"\" + ClientName + @"_Report\Full\";
        if (server == @"LONINFSQLC03V2\INST2")
        {
            clientDbBackupPath = @"\\lonrpxflr01\LONRMSSQLBackup01";
        }
        else
        {
            clientDbBackupPath = @"\\lonrpxflr01\LONRMSSQLBackup02";

        }
        GetLatestFullBackup(ClientName, clientDbBackupPath, out fullFileName, "BAK", server, fullFileName);
        GetLatestFullBackup(ClientName, clientDbBackupPath, out diffFileName, "DFR", server, fullFileName);
        GetReportBackup(reportDbBackupPath, out reportFullBackName);
    }
    //dbRestore.PercentComplete += new PercentCompleteEventHandler(restore_PercentComplete);

    if (diffFileName == "")
    {
        dbRestore.NoRecovery = false;
    }
    else
    {
        dbRestore.NoRecovery = true;
    }
    //\\atlsqlbkup02\ATLRMSETLBackup\servername\clientreportdbname\full\
    string dataFile;
    string logFile;

    GetFileList(server, clientDbBackupPath, ClientName, out dataFile, out logFile, fullFileName, "client");

    BackupDeviceItem bdiFullBackup = default(BackupDeviceItem);
    bdiFullBackup = new BackupDeviceItem(clientDbBackupPath + @"\" + server.Replace(@"\", "$") + @"\" + ClientName + @"\FULL\" + fullFileName, DeviceType.File);
    dbRestore.Devices.Add(bdiFullBackup);

    RelocateFile reloData = new RelocateFile(dataFile, @"e:\Data\" + ClientName + "_Export_Data.mdf");
    RelocateFile reloLog = new RelocateFile(logFile, @"e:\Data\" + ClientName + "_Export_Log.ldf");

    dbRestore.RelocateFiles.Add(reloData);
    dbRestore.RelocateFiles.Add(reloLog);

    dbRestore.ReplaceDatabase = true;

    destinationServer.ConnectionContext.StatementTimeout = 0;

    dbRestore.SqlRestore(destinationServer);

    dbRestore.Devices.Remove(bdiFullBackup);
    if (diffFileName != "")
    {
        BackupDeviceItem bdiDiffBackup = default(BackupDeviceItem);
        bdiDiffBackup = new BackupDeviceItem(clientDbBackupPath + @"\" + server.Replace(@"\", "$") + @"\" + ClientName + @"\DIFF\" + diffFileName, DeviceType.File);
        dbRestore.Devices.Add(bdiDiffBackup);
        dbRestore.NoRecovery = false;


        dbRestore.SqlRestore(destinationServer);
        //logRequest(request.RequestID, "Status", "Restore Complete");
    }
    destinationServer.Databases.Refresh();
    destinationServer.Databases[exportClientName].DatabaseOptions.RecoveryModel = RecoveryModel.Simple;
    destinationServer.Databases[exportClientName].DatabaseOptions.AnsiNullDefault = false;
    destinationServer.Databases[exportClientName].DatabaseOptions.RecursiveTriggersEnabled = false;
    destinationServer.Databases[exportClientName].DatabaseOptions.PageVerify = PageVerify.None;
    destinationServer.Databases[exportClientName].DatabaseOptions.UserAccess = DatabaseUserAccess.Multiple;
    destinationServer.Databases[exportClientName].DatabaseOptions.AutoClose = false;
    destinationServer.Databases[exportClientName].DatabaseOptions.AutoShrink = false;
    destinationServer.Databases[exportClientName].DatabaseOptions.AutoCreateStatistics = true;
    destinationServer.Databases[exportClientName].DatabaseOptions.AutoUpdateStatistics = true;
    destinationServer.Databases[exportClientName].DatabaseOptions.QuotedIdentifiersEnabled = false;


    destinationServer.Databases[exportClientName].Alter();

    SqlConnectionStringBuilder reportConnStr = new SqlConnectionStringBuilder();
    reportConnStr.DataSource = etlServer;
    reportConnStr.InitialCatalog = "master";
    reportConnStr.IntegratedSecurity = true;

    SqlConnection reportConnection = new SqlConnection(reportConnStr.ConnectionString);

    reportConnection.Open();

    SqlCommand reportCmd = new SqlCommand();
    reportCmd.CommandTimeout = 0;
    GetFileList(etlServer, reportDbBackupPath, ClientName + "_Report", out dataFile, out logFile, reportFullBackName, "report");


    reportCmd.CommandText = @"RESTORE DATABASE " + exportReportName + @"
                                      FROM DISK = '" + reportDbBackupPath + reportFullBackName + @"'
                                      WITH MOVE '" + dataFile + @"' TO 'e:\Data\" + ClientName + @"_Report_Export_Data.mdf', 
                                           MOVE '" + logFile + @"' TO'e:\Data\" + ClientName + @"_Report_Export_log.ldf'";

    reportCmd.Connection = reportConnection;

    reportCmd.ExecuteNonQuery();

    sqlConn.Close();
    reportConnection.Close();

}

static void GetProdClientLocation(string ClientName, out string clientServer, string environment)
{
    {
        clientServer = "";
        try
        {

            int servercnt = 1;
            if (environment == "ATL")
            {
                while (servercnt <= 4)
                {
                    SqlConnection etlConnection = new SqlConnection();
                    SqlConnectionStringBuilder etlConnectionStr = new SqlConnectionStringBuilder();

                    string serverName = @"ATLINFSQLC01V" + servercnt.ToString() + @"\INST" + servercnt.ToString();
                    Console.WriteLine(@"server: " + serverName);

                    etlConnectionStr.DataSource = serverName;
                    etlConnectionStr.InitialCatalog = @"master";
                    etlConnectionStr.IntegratedSecurity = true;
                    Console.WriteLine(@"pre etl connection");
                    etlConnection.ConnectionString = etlConnectionStr.ConnectionString;
                    etlConnection.Open();

                    SqlCommand etlCommand = new SqlCommand();
                    etlCommand.CommandText = @"select name from sys.databases where name = '" + ClientName + @"'";
                    etlCommand.Connection = etlConnection;

                    SqlDataReader etlReader = etlCommand.ExecuteReader();

                    if (etlReader.HasRows)
                    {
                        clientServer = serverName;

                    }
                    servercnt++;

                }
            }
            else
            {
                servercnt = 1;
                while (servercnt <= 3)
                {
                    SqlConnection etlConnection = new SqlConnection();
                    SqlConnectionStringBuilder etlConnectionStr = new SqlConnectionStringBuilder();

                    string serverName = @"LONINFSQLC03V" + servercnt.ToString() + @"\INST" + servercnt.ToString();
                    Console.WriteLine(@"server: " + serverName);

                    etlConnectionStr.DataSource = serverName;
                    etlConnectionStr.InitialCatalog = @"master";
                    etlConnectionStr.IntegratedSecurity = true;
                    Console.WriteLine(@"pre lon server connect");

                    etlConnection.ConnectionString = etlConnectionStr.ConnectionString;
                    etlConnection.Open();

                    SqlCommand etlCommand = new SqlCommand();
                    etlCommand.CommandText = @"select name from sys.databases where name = '" + ClientName + @"'";
                    etlCommand.Connection = etlConnection;

                    SqlDataReader etlReader = etlCommand.ExecuteReader();

                    if (etlReader.HasRows)
                    {
                        clientServer = serverName;

                    }
                    servercnt++;

                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            Console.WriteLine("find prod server");
            throw ex;
        }
    }
}


static void RestoreDatabases(string ClientName, out string exportServer, string environment)
{
    //determine prod environment --
    //determine etl server  lon loninfsqlc03v4\inst4 atlanta atlrp1etl11-14( need code for ATL)
    //determine prod client server
    //using destination environment -- default prod, ri1\inst5 for testing
    //restore report db with custom name
    //restore client db with custom name

    string server;
    server = @"";
    exportServer = @"";
    string dataCenter = "";

    Console.WriteLine(@"pre get prod location");
    GetProdClientLocation(ClientName, out server, environment);

    //clientserver = server

    if (server == @"LONINFSQLC03V2\INST2" || server == @"LONINFSQLC03V3\INST3")
    {
        exportServer = @"LONINFSQLC03V4\INST4";
        dataCenter = "LON";

    }
    else
    {
        FindATLetlServer(ClientName, out exportServer);
        dataCenter = "ATL";

    }

    //restore dbs on etl server
    RestoreFullDatabase(exportServer, ClientName, server, dataCenter);

    


    static void FindATLetlServer(string ClientName, out string exportServer)

    {

        exportServer = @"";
        int servercnt = 11;
        try
        {
            while (servercnt <= 15)
            {
                SqlConnection etlConnection = new SqlConnection();
                SqlConnectionStringBuilder etlConnectionStr = new SqlConnectionStringBuilder();

                string serverName = @"ATLRP1ETL" + servercnt.ToString();

                etlConnectionStr.DataSource = serverName;
                etlConnectionStr.InitialCatalog = @"master";
                etlConnectionStr.IntegratedSecurity = true;

                etlConnection.ConnectionString = etlConnectionStr.ConnectionString;
                etlConnection.Open();

                SqlCommand etlCommand = new SqlCommand();
                etlCommand.CommandText = @"select name from sys.databases where name = '" + ClientName + @"'";
                etlCommand.Connection = etlConnection;

                SqlDataReader etlReader = etlCommand.ExecuteReader();

                if (etlReader.HasRows)
                {
                    exportServer = serverName;

                }
                servercnt++;

            }

        }
        catch (Exception ex)
        {

            Console.WriteLine(ex.Message);
            StreamWriter errorfile = new StreamWriter(@".\errors_" + ClientName + @".txt");
            errorfile.WriteLine(ex.ToString());
            errorfile.WriteLine(ex.Message);
            //errorfile.WriteLine(ex.InnerException.ToString());
            errorfile.Close();
            errorfile.Dispose();
            
        }
    }
}

    
    


public class candRecord
{
    public int CANDID { get; set; }
    public int CANDPROFILEID { get; set; }
    public string ADDRESS1 { get; set; }
    public string CREATEDON { get; set; }
    public string PRIMARYEMAILADDRESS { get; set; }
    public string NAME { get; set; }
    public string CLIENTREQID { get; set; }
    public string SOURCEAVENUEVALUE { get; set; }
    public string FOLDER { get; set; }
    public string FOLDERDISPLAYTEXT { get; set; }
    public string MODIFIEDON { get; set; }
    public string WORKFLOW { get; set; }
    public string WORKFLOWDISPLAYTEXT { get; set; }
    public string PHONE { get; set; }
    public string GENDER { get; set; }
    public string Ethnicity { get; set; }
    public string VeteranStatus { get; set; }
    public string DISABILITY { get; set; }
    public string source { get; set; }

}
public class reqRecord
{

    public string CLIENTREQID { get; set; }
    public string CREATEDON { get; set; } 
    public string CREATE_USER { get; set; }
    public string DESCRIPTION { get; set; }
    public string EducationType { get; set; }
    public string RECRUITSTARTDATE { get; set; }
    public string RECRUITENDDATE { get; set; }
    public string CITY { get; set; }
    public string COUNTRYCODE { get; set; }
    public string LOCATIONDISPLAYTEXT { get; set; }
    public string STATEPROVINCE { get; set; }
    public string TITLE { get; set; }
    public string WORKFLOWDISPLAYTEXT { get; set; }
    public string EDUCATIONDISPLAYTEXT { get; set; }
    public string MODIFIEDON { get; set; }
    public string ReqStatus { get; set; }
    public string EDUCATION { get; set; }
    public string EXPIERENCE { get; set; }
    public string POSITIONTYPE { get; set; }
    public string POSITIONCATEGORY { get; set; }
    public string DEPARTMENT { get; set; }
    public string SalaryLow { get; set; }
    public string SalaryHigh { get; set; }

}



public class candEmploymentRecord {
    public int candid  { get; set; }
    public int candprofileid { get; set; }
    public string companyName { get; set; }
    public string employmentStartDate { get; set; }
    public string employmentEndDate { get; set; }

}

//cast(cp.candid as nvarchar(10)) as CANDID, cast(cp.CandProfileID as nvarchar(10))
//         as CANDPROFILEID, replace(rtrim(CollegeNameFactCode), '""', '\""') as COLLEGENAME,
//        +replace(rtrim(CollegeDegreeFactCode), '""', '\""') + as COLLEGEDEGREE,
//        +replace(rtrim(CollegeMajorFactCode), '""', '\""') + as COLLEGEMAJOR, 
//        +Format(GraduationDate, 'yyyy-MM-dd hh:mm:ss tt') + as GRADUATIONDATE

public class candEducationRecord
{
    public int candid { get; set; }
    public int candprofileid { get; set; }
    public string collegeName { get; set; }
    public string collegeDegree { get; set; }
    public string collegeMajor { get; set; }
    public string graduationDate { get; set; }
}
public class emailRecord
{

    public int candid { get; set;}
    public int candprofileid { get; set; }
    public string subject { get; set; }
    public string sentOn { get; set; }
    public string bodyText { get; set; }
    public string bodyHTML { get; set; }


}

public class condFieldRecord
{
    public string displaytext { get; set; }
    public string condField { get; set; }

}


public class reqUserRoleRecord
{
    public string clientReqID { get; set; }
    public string email { get; set; }
    public string role { get; set; }
    public string type { get; set; }



 
}

public class APSLF
{
    public string ParentRMSID { get; set; }
    public string ParentRMSCode { get; set; }
    public string Parent { get; set; }
    public string ChildRMSID { get; set; }
    public string ChildRMSCode { get; set; }
    public string Child { get; set; }
    public string ParentValue { get; set; }
    public string ParentActive { get; set; }
    public string ChildValue { get; set; }
    public string ChildActive { get; set; }
    public string RelationshipType { get; set; }


}

public class attachment
{
    public int CANDIDATEID { get; set; }
    public int CANDPROFILEID { get; set; }
    public string CLIENTREQID { get; set; }
    public string filename { get; set; }
    public string UIFilename { get; set; }
    public string AttachmentType { get; set; }


}