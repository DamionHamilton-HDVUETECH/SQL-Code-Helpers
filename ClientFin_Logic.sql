


-- Develop notes about the Clin. Fin process and how this operate. 



---********************* Begining Income Query *********************----

---add filter where systemiD is not null
--List all Records that are within the 5 Year date range
IF OBJECT_ID(N'tempdb..#Financials_5Yr') IS NOT NULL
BEGIN
DROP TABLE #Financials_5Yr;
END;
 --drop table #Financials_5Yr
--Creating 5yr data range varables to be used downstream
DECLARE @StartDate_5yr DATE = DateAdd(yy, -5, GetDate());
DECLARE @EndDate_5yr DATE = GetDate();

--Statment end points
DECLARE @Current_Statment_Date_EndPoint DATE = DateAdd(mm, -12, GetDate());
DECLARE @Prior_Statment_Date_EndPoint DATE = DateAdd(mm, -24, GetDate());
DECLARE @Report_Date DATE = GetDate();


print Concat('Current Statment Endpoint Date: ', @Current_Statment_Date_EndPoint)
print Concat('Report Date: ', @Report_Date)
print Concat('Prior Statment Endpoint Date: ', @Prior_Statment_Date_EndPoint)



select 
 -- ROW_NUMBER() Over(order By pkid_) As 'Row_ID',
  *,
  CASE
  	WHEN auditmethod = 'Unqualif''d' THEN 1 
  	WHEN auditmethod = 'Qualified' THEN 2 
  	WHEN auditmethod = 'Reviewed' THEN 3 
  	WHEN auditmethod = 'Compiled' THEN 4 						
  	WHEN auditmethod = 'Co.Prep''d' THEN 5 
  	END AS 'Audit_Rank',
  CASE
  	WHEN statementtype = 'Annual' THEN 1 
  	WHEN statementtype = 'Quarterly' THEN 2 
  	WHEN statementtype = 'Monthly' THEN 3 						
  	END AS 'StatmentType_Rank'
	into #Financials_5Yr
  	from 
		(
			 --Get Current Trailing Twelve Month data Within the 5yr Range
			 --IF object_id('#tempdb..#tempD') is not null drop table #tempD
				
				--STEP 3 Return pkid,entityid,statmentID..ect to include a 5yr range filter along with a current year filter for current year data. 
				 select A.ASOfDate, CONCAT(A.ENTITYID,A.STATEMENTID) as pkid_, A.entityid, A.statementid, A.statementdatekey_, A.auditmethod, A.statementmonths, A.statementtype
							--,B.[Date of Financials _ Max statmentdate]
							--into #tempD
					  From [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS] A
					  left Join
					  (
							--STEP 2 Return max date by entityid and Pkid
						  Select distinct a.entityid, CONCAT(a.ENTITYID,a.STATEMENTID) as pkid_, a.statementdatekey_  AS 'Date of Financials _ Max statmentdate'
							from [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS] a
							 right join (
								--STEP 1 Return Max statmentid group by entityid
							  select Max(statementdatekey_) AS 'Max_StatmentDateKey',Entityid
							  from [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS]
							  --where entityid = 2915			  
							  where AsOfDate = (select max(Cast(AsOfDate as date)) From [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS])
							  group by entityid
							  ) b
								ON a.entityid = b.Entityid and a.statementdatekey_ = b.Max_StatmentDateKey
								where a.AsOfDate = (select max(Cast(AsOfDate as date)) From [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS])
					
					) B
					ON CONCAT(A.ENTITYID,A.STATEMENTID) = B.pkid_ and A.entityid = B.entityid					 
					  where CONCAT(A.ENTITYID,A.STATEMENTID) is not null
					  and A.AsOfDate = (select max(Cast(AsOfDate as date)) From [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS])
					 -- and A.statementdatekey_ between @Current_Statment_Date_EndPoint and @Report_Date	-- For Current year values.
					  --and A.entityid = 1316--7070 --6336 --1316--2915
					 -- order by statementdatekey_ DESC
		) Current_Income

		order by Current_Income.statementdatekey_ DESC
	
	--select distinct * 
	--from #Financials_5Yr where entityid in (31395,50738,51721,51879)


	--	select distinct B.systemid, A.* 
	--from #Financials_5Yr A
	--left join (
	--	select distinct systemid, entityid from [AXIOM_14Q].[CreditLens].[STG_BP_Factentity_Hist]
	--) B
	--	on A.entityid = B.entityid
	--	order by systemid DESC

---------------------********* Create a temp Entity ID List for all data within 5yr span *****************************************************	
	IF OBJECT_ID(N'tempdb..#Temp_Dist_Ids') IS NOT NULL
BEGIN
DROP TABLE #Temp_Dist_Ids;
END;
	--drop table #Temp_Dist_Ids
	select distinct entityid
	into #Temp_Dist_Ids
	from #Financials_5Yr
	where auditmethod in ('Unqualif''d',
						  'Qualified',
						  'Reviewed',
						  'Compiled',
						  'Co.Prep''d'
						  ) --- apply autitmethod filter          
						  and statementtype NOT IN ('FY-To-Date','Rolling Stmt')



	------------------------------------------------------------------------------------------
-------------------------------  Annual Check Run ---- Select all where statmentmonths is 12 --------------------------------------------------------
	
	IF OBJECT_ID(N'tempdb..#Annual_Financials_5Yr_with_Ranked_order_By_statmentdate') IS NOT NULL
BEGIN
DROP TABLE #Annual_Financials_5Yr_with_Ranked_order_By_statmentdate;
END;
	--drop table #Annual_Financials_5Yr_with_Ranked_order_By_statmentdate
	
		select
		distinct
		convert(date,statementdatekey_) AS 'Statment_date',
		statementdatekey_,
		pkid_,
		entityid,
		auditmethod,
		statementmonths,
		statementtype,
		Audit_Rank,
		StatmentType_Rank,
		ROW_NUMBER() OVER (PARTITION BY entityid ORDER BY statementdatekey_ DESC) ranked_order,
		'' AS 'CF_Type'
		--'' AS 'Q_Run',
		--'' AS 'M_Run'
		into #Annual_Financials_5Yr_with_Ranked_order_By_statmentdate
		From
					(
					select distinct 
					convert(date,statementdatekey_) AS 'Statment_date',
					*
					from #Financials_5Yr
					Where 
					--ENTITYID = 51350 and
					auditmethod in (
											 'Unqualif''d',
											 'Qualified',
											 'Reviewed',
											 'Compiled',
											 'Co.Prep''d'
											) --- apply autitmethod filter          
											and statementtype NOT IN ('FY-To-Date','Rolling Stmt')
											and pkid_
											in(
											select					
												max(pkid_) as pkid_
												from #Financials_5Yr
												Where auditmethod in (
																	 'Unqualif''d',
																	 'Qualified',
																	 'Reviewed',
																	 'Compiled',
																	 'Co.Prep''d'
																	) --- apply autitmethod filter          
																	and statementtype NOT IN ('FY-To-Date','Rolling Stmt')
																	and entityid in(select entityid from #Temp_Dist_Ids)
																	group by entityid, statementdatekey_, statementtype
												)
					) IB
		where 
		statementmonths = '12' 
		--and entityid in ('2915','7070','6336','1316')
		--Order by statementdatekey_ DESC

		
		
--		select * from #Annual_Financials_5Yr_with_Ranked_order_By_statmentdate where entityid = 51350

----------------------------------------------------------------------------


-------------------------------------------  Mark latest 24 months worth of data as Annual using A for downstream filtering. -------------------------------------------------------

--Check for Annual Statments group by entity id and update A_Run to A to represent Annual
--- Then output entity and pkid where statment date is latest two 12 month statments this will represent a 24 month range of data for each entityid

 --DECLARE @Prior_Statment_Date_EndPoint DATE 
 --SET @Prior_Statment_Date_EndPoint = DateAdd(mm, -24, GetDate());

DECLARE @entityid varchar(20)
DECLARE @statmentdatekey_ Date 
DECLARE @statementtype varchar(20)
DECLARE @statementmonths int



DECLARE Ent_Cursor CURSOR FOR
select entityid, statementdatekey_, statementmonths, statementtype from #Annual_Financials_5Yr_with_Ranked_order_By_statmentdate

Open Ent_Cursor
Fetch next from Ent_Cursor INTO @entityid, @statmentdatekey_, @statementmonths, @statementtype

While @@Fetch_Status = 0

BEGIN
	Update #Annual_Financials_5Yr_with_Ranked_order_By_statmentdate
	Set CF_Type = 'A'
	where ranked_order < 3
	--Where statementmonths = 12 and statementdatekey_ >= @Prior_Statment_Date_EndPoint

	Fetch Next From Ent_Cursor INTO @entityid, @statmentdatekey_, @statementmonths, @statementtype
END

CLOSE Ent_Cursor
DEALLOCATE Ent_Cursor

--select * from #Annual_Financials_5Yr_with_Ranked_order_By_statmentdate --where entityid = '6336' and statementmonths = 12
--order by entityid ASC


----------------- Select latest 24 months worth of data for each entity where found

	IF OBJECT_ID(N'tempdb..#Temp_Annuals_24') IS NOT NULL
BEGIN
DROP TABLE #Temp_Annuals_24;
END;
--drop table #Temp_Annuals_24
select 
entityid, 
pkid_, 
statementdatekey_, 
convert(date,statementdatekey_) AS 'Statment_date',
statementtype,
statementmonths,
CF_Type,
ranked_order
--Q_Run,
--M_Run
into #Temp_Annuals_24
from #Annual_Financials_5Yr_with_Ranked_order_By_statmentdate
where CF_Type = 'A' 

				and entityid in (
								select entityid from #Annual_Financials_5Yr_with_Ranked_order_By_statmentdate
								where CF_Type = 'A'
								group by entityid
								Having sum(Convert(INT,statementmonths)) = 24
								)

	--select distinct entityid from #Temp_Annuals_24	
	--order by entityid DESC

	--select * from #Temp_Annuals_24

-- Remove all entity Ids with Annual Financials found to represent latest two years from entity ID list, 
-----  If only found 12 months of data for enitiy Id then this ID will not be removed from list it will continue in list and be used on next Quarterly , Monthly runs.  
Delete from #Temp_Dist_Ids
where entityid in (select distinct entityid from #Temp_Annuals_24 group by entityid having count(*) > 1)

----- Remove all entity Ids from Annual table where only 1 statment is found because its not 24 months
--Delete from #Temp_Annuals_24
--where entityid in (select distinct entityid from #Temp_Annuals_24 group by entityid having count(*) = 1)

--select * from #Temp_Annuals_24	
--	order by entityid DESC


-------------------------------  Monthly Check Run ---- Select all where statmentmonths is 1 --------------------------------------------------------

IF OBJECT_ID(N'tempdb..#Monthly_Financials_5Yr_with_Ranked_order_By_statmentdate') IS NOT NULL
BEGIN
DROP TABLE #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdate;
END;
--drop table #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdate
	
		select
		distinct
		convert(date,statementdatekey_) AS 'Statment_date',
		statementdatekey_,
		pkid_,
		entityid,
		auditmethod,
		statementmonths,
		statementtype,
		Audit_Rank,
		StatmentType_Rank,
		ROW_NUMBER() OVER (PARTITION BY entityid ORDER BY statementdatekey_ DESC) ranked_order,
		--'' AS 'A_Run',
		--'' AS 'Q_Run'
		'' AS 'CF_Type'
		into #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdate
		From
					(
					select distinct 
					convert(date,statementdatekey_) AS 'Statment_date',
					*
					from #Financials_5Yr
					Where auditmethod in (
											 'Unqualif''d',
											 'Qualified',
											 'Reviewed',
											 'Compiled',
											 'Co.Prep''d'
											) --- apply autitmethod filter          
											and statementtype NOT IN ('FY-To-Date','Rolling Stmt')
											and pkid_
											in(
											select					
												max(pkid_) as pkid_
												from #Financials_5Yr
												Where auditmethod in (
																	 'Unqualif''d',
																	 'Qualified',
																	 'Reviewed',
																	 'Compiled',
																	 'Co.Prep''d'
																	) --- apply autitmethod filter          
																	and statementtype NOT IN ('FY-To-Date','Rolling Stmt')
																	--and entityid in(select entityid from #Temp_Dist_Ids)
																	group by entityid, statementdatekey_,statementtype
												)
					) IB
		where entityid IN (Select * from #Temp_Dist_Ids)
		and statementmonths = '1' 
		
		--select distinct entityid from #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdate		
		--where entityid = 13320
		--order by entityid, ranked_order ASC
		


-------------------------------------------  Mark latest 24 months worth of data as Monthly using A for downstream filtering. -------------------------------------------------------

--Check for Annual Statments group by entity id and update A_Run to A to represent Annual
--- Then output entity and pkid where statment date is latest two 12 month statments this will represent a 24 month range of data for each entityid


DECLARE @entityid2 varchar(20)
DECLARE @statmentdatekey_2 Date 
DECLARE @statementtype2 varchar(20)
DECLARE @statementmonths2 int
DECLARE @Months2 int



DECLARE Ent_Cursor CURSOR FOR
select entityid, statementdatekey_, statementmonths, statementtype from #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdate

Open Ent_Cursor
Fetch next from Ent_Cursor INTO @entityid2, @statmentdatekey_2, @statementmonths2, @statementtype2

While @@Fetch_Status = 0

BEGIN
	Update #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdate
	Set CF_Type = 'B'
	where ranked_order < 25 --and entityid = 13320
	--Where statementmonths = 12 and statementdatekey_ >= @Prior_Statment_Date_EndPoint
	--set @Months = Cast(@statementmonths as int)

	--print DATEADD(month, -@Months,  Cast(@statmentdatekey_ as date))



	Fetch Next From Ent_Cursor INTO @entityid2, @statmentdatekey_2, @statementmonths2, @statementtype2
END

CLOSE Ent_Cursor
DEALLOCATE Ent_Cursor



----------------- Select latest 24 months worth of data for each entity where found


IF OBJECT_ID(N'tempdb..#Temp_Monthly_24') IS NOT NULL
BEGIN
DROP TABLE #Temp_Monthly_24;
END;
--drop table #Temp_Monthly_24
select 
entityid, 
pkid_, 
statementdatekey_, 
convert(date,statementdatekey_) AS 'Statment_date',
statementtype,
statementmonths,
ranked_order,
--Q_Run,
CF_Type
into #Temp_Monthly_24
from #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdate
where CF_Type = 'B' 

				and entityid in (
								select entityid from #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdate
								where CF_Type = 'B'
								group by entityid
								Having sum(Convert(INT,statementmonths)) = 24
								)

	--select distinct entityid from #Temp_Monthly_24	
	--where entityid = 14568
	--order by entityid,ranked_order DESC

	
-- Remove all entity Ids with Monthly Financials found to represent latest two years from entity ID list, 
-----  If only found 12 months of data for enitiy Id then this ID will not be removed from list it will continue in list and be used on next Quarterly , Monthly runs.  
Delete from #Temp_Dist_Ids
where entityid in (select distinct entityid from #Temp_Monthly_24 group by entityid having count(*) > 1)



--**********************************************************

-------------------------------  Monthly Check Run ---- Select all where statmentmonths is 1 --------------------------------------------------------


IF OBJECT_ID(N'tempdb..#Monthly_Financials_5Yr_with_Ranked_order_By_statmentdatea_Two') IS NOT NULL
BEGIN
DROP TABLE #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdatea_Two;
END;
--drop table #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdatea_Two
	
		select
		distinct
		convert(date,statementdatekey_) AS 'Statment_date',
		statementdatekey_,
		pkid_,
		entityid,
		auditmethod,
		statementmonths,
		statementtype,
		Audit_Rank,
		StatmentType_Rank,
			'' AS 'CF_Type',
		ROW_NUMBER() OVER (PARTITION BY entityid ORDER BY statementdatekey_ DESC) ranked_order
		
		into #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdatea_Two
		From
					(
					select distinct 
					convert(date,statementdatekey_) AS 'Statment_date',
					*
					from #Financials_5Yr
					Where auditmethod in (
											 'Unqualif''d',
											 'Qualified',
											 'Reviewed',
											 'Compiled',
											 'Co.Prep''d'
											) --- apply autitmethod filter          
											and statementtype NOT IN ('FY-To-Date','Rolling Stmt')

											and pkid_
											in(
											select					
												max(pkid_) as pkid_
												from #Financials_5Yr
												Where auditmethod in (
																	 'Unqualif''d',
																	 'Qualified',
																	 'Reviewed',
																	 'Compiled',
																	 'Co.Prep''d'
																	) --- apply autitmethod filter          
																	and statementtype NOT IN ('FY-To-Date','Rolling Stmt')
																	--and entityid = 51350
																	group by entityid, statementdatekey_,statementtype
												)
					) IB
		where entityid IN (Select * from #Temp_Dist_Ids)
		and statementmonths = '1' 
		--and entityid = 14568
		--select * from #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdatea_Two		
		--where entityid = 14568
		--order by entityid, ranked_order ASC
		


-------------------------------------------  Mark latest 12 months worth of data as Monthly using A for downstream filtering. -------------------------------------------------------

--Check for Annual Statments group by entity id and update A_Run to A to represent Annual
--- Then output entity and pkid where statment date is latest two 12 month statments this will represent a 24 month range of data for each entityid


DECLARE @entityid3 varchar(20)
DECLARE @statmentdatekey_3 Date 
DECLARE @statementtype3 varchar(20)
DECLARE @statementmonths3 int
DECLARE @Months3 int



DECLARE Ent_Cursor CURSOR FOR
select entityid, statementdatekey_, statementmonths, statementtype from #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdatea_Two

Open Ent_Cursor
Fetch next from Ent_Cursor INTO @entityid3, @statmentdatekey_3, @statementmonths3, @statementtype3

While @@Fetch_Status = 0

BEGIN
	Update #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdatea_Two
	Set CF_Type = 'D'
	where ranked_order < 13
	--Where statementmonths = 12 and statementdatekey_ >= @Prior_Statment_Date_EndPoint
	--set @Months = Cast(@statementmonths as int)

	--print DATEADD(month, -@Months,  Cast(@statmentdatekey_ as date))



	Fetch Next From Ent_Cursor INTO @entityid3, @statmentdatekey_3, @statementmonths3, @statementtype3
END

CLOSE Ent_Cursor
DEALLOCATE Ent_Cursor

--select * from #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdatea_Two
--where M_Run_12 = 'M'


----------------- Select latest 24 months worth of data for each entity where found

IF OBJECT_ID(N'tempdb..#Temp_Monthly_12') IS NOT NULL
BEGIN
DROP TABLE #Temp_Monthly_12;
END;
--drop table #Temp_Monthly_12
select 
entityid, 
pkid_, 
statementdatekey_, 
convert(date,statementdatekey_) AS 'Statment_date',
statementtype,
statementmonths,
CF_Type,
ranked_order
--Q_Run,

into #Temp_Monthly_12
from #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdatea_Two
where CF_Type = 'D' 

				and entityid in (
								select entityid from #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdatea_Two
								where CF_Type = 'D'
								group by entityid
								Having sum(Convert(INT,statementmonths)) = 12
								)

	--select distinct entityid from #Temp_Monthly_12	
	--order by entityid,ranked_order DESC

	
-- Remove all entity Ids with Monthly Financials found to represent latest two years from entity ID list, 
-----  If only found 12 months of data for enitiy Id then this ID will not be removed from list it will continue in list and be used on next Quarterly , Monthly runs.  
Delete from #Temp_Dist_Ids
where entityid in (select distinct entityid from #Temp_Monthly_12 group by entityid having count(*) > 1)


--************************************************************






------------------------------------------------------------------------------------
IF OBJECT_ID(N'tempdb..#Only_12_found_for_12_Months') IS NOT NULL
BEGIN
DROP TABLE #Only_12_found_for_12_Months;
END;
--drop table #Only_12_found_for_12_Months
	
		select
		distinct
		convert(date,statementdatekey_) AS 'Statment_date',
		statementdatekey_,
		pkid_,
		entityid,
		auditmethod,
		statementmonths,
		statementtype,
		Audit_Rank,
		StatmentType_Rank,
		ROW_NUMBER() OVER (PARTITION BY entityid ORDER BY statementdatekey_ DESC) ranked_order,
		--'' AS 'A_Run',
		--'' AS 'Q_Run'
		'' AS 'CF_Type'
		into #Only_12_found_for_12_Months
		From
					(
					select distinct 
					convert(date,statementdatekey_) AS 'Statment_date',
					*
					from #Financials_5Yr
					Where auditmethod in (
											 'Unqualif''d',
											 'Qualified',
											 'Reviewed',
											 'Compiled',
											 'Co.Prep''d'
											) --- apply autitmethod filter          
											and statementtype NOT IN ('FY-To-Date','Rolling Stmt')
											and pkid_
											in(
											select					
												max(pkid_) as pkid_
												from #Financials_5Yr
												Where auditmethod in (
																	 'Unqualif''d',
																	 'Qualified',
																	 'Reviewed',
																	 'Compiled',
																	 'Co.Prep''d'
																	) --- apply autitmethod filter          
																	and statementtype NOT IN ('FY-To-Date','Rolling Stmt')
																	--and entityid = 51350
																	group by entityid, statementdatekey_,statementtype
												)

					) IB
		where entityid IN (Select * from #Temp_Dist_Ids)
		



------------------------------------------------------------  Only 12 Months Found

IF OBJECT_ID(N'tempdb..#Temp_Annuals_12') IS NOT NULL
BEGIN
DROP TABLE #Temp_Annuals_12;
END;
--drop table #Temp_Annuals_12
select
entityid, 
pkid_, 
statementdatekey_, 
convert(date,statementdatekey_) AS 'Statment_date',
statementtype,
statementmonths,
'C' AS 'CF_Type',
1 AS 'ranked_order'

into #Temp_Annuals_12
from #Only_12_found_for_12_Months
where pkid_ in (
								select pkid_ from #Only_12_found_for_12_Months
								where statementmonths = 12								
								group by entityid,pkid_								
								Having sum(Convert(INT,statementmonths)) = 12

)

--select distinct entityid from #Temp_Annuals_12
--order by entityid DESC

-- Remove all entity Ids with Annual Financials found to represent latest One year from entity ID list, 
-----
Delete from #Temp_Dist_Ids
where entityid in (select distinct entityid from #Temp_Annuals_12 group by entityid)



----------------------------------********************* Quartley 24 Month Run ******************************

IF OBJECT_ID(N'tempdb..#Quaterly_Run_24') IS NOT NULL
BEGIN
DROP TABLE #Quaterly_Run_24;
END;
--drop table #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdate
	
		select
		distinct
		convert(date,statementdatekey_) AS 'Statment_date',
		statementdatekey_,
		pkid_,
		entityid,
		auditmethod,
		statementmonths,
		statementtype,
		Audit_Rank,
		StatmentType_Rank,
		ROW_NUMBER() OVER (PARTITION BY entityid ORDER BY statementdatekey_ DESC) ranked_order,	
		'' AS 'CF_Type'
		into #Quaterly_Run_24
		From
					(
					select distinct 
					convert(date,statementdatekey_) AS 'Statment_date',
					*
					from #Financials_5Yr
					Where auditmethod in (
											 'Unqualif''d',
											 'Qualified',
											 'Reviewed',
											 'Compiled',
											 'Co.Prep''d'
											) --- apply autitmethod filter          
											and statementtype NOT IN ('FY-To-Date','Rolling Stmt')
											and pkid_
											in(
											select					
												max(pkid_) as pkid_
												from #Financials_5Yr
												Where auditmethod in (
																	 'Unqualif''d',
																	 'Qualified',
																	 'Reviewed',
																	 'Compiled',
																	 'Co.Prep''d'
																	) --- apply autitmethod filter          
																	and statementtype NOT IN ('FY-To-Date','Rolling Stmt')
																	--and entityid = 51350
																	group by entityid, statementdatekey_, statementtype
												)

					) IB
		where entityid IN (Select * from #Temp_Dist_Ids)
		and statementmonths = '3' 

--select * from #Quaterly_Run_24 where entityid = 34904	

-------------------------------------------  Mark latest 24 months worth of data as Monthly using Q for downstream filtering. -------------------------------------------------------

--Check for Annual Statments group by entity id and update Q_Run to Q to represent Annual

DECLARE @entityid4 varchar(20)
DECLARE @statmentdatekey_4 Date 
DECLARE @statementtype4 varchar(20)
DECLARE @statementmonths4 int
DECLARE @Months4 int



DECLARE Ent_Cursor CURSOR FOR
select entityid, statementdatekey_, statementmonths, statementtype from #Quaterly_Run_24

Open Ent_Cursor
Fetch next from Ent_Cursor INTO @entityid4, @statmentdatekey_4, @statementmonths4, @statementtype4

While @@Fetch_Status = 0

BEGIN
	Update #Quaterly_Run_24
	Set CF_Type = 'F'
	where ranked_order < 9 --and entityid = 13320
	--Where statementmonths = 12 and statementdatekey_ >= @Prior_Statment_Date_EndPoint
	--set @Months = Cast(@statementmonths as int)

	--print DATEADD(month, -@Months,  Cast(@statmentdatekey_ as date))



	Fetch Next From Ent_Cursor INTO @entityid4, @statmentdatekey_4, @statementmonths4, @statementtype4
END

CLOSE Ent_Cursor
DEALLOCATE Ent_Cursor



----------------- Select latest 24 months worth of data for each entity where found


IF OBJECT_ID(N'tempdb..#Temp_Quaterly_24') IS NOT NULL
BEGIN
DROP TABLE #Temp_Quaterly_24;
END;
--drop table #Temp_Monthly_24
select 
entityid, 
pkid_, 
statementdatekey_, 
convert(date,statementdatekey_) AS 'Statment_date',
statementtype,
statementmonths,
ranked_order,
--Q_Run,
CF_Type
into #Temp_Quaterly_24
from #Quaterly_Run_24
where CF_Type = 'F' 

				and entityid in (
								select entityid from #Quaterly_Run_24
								where CF_Type = 'F'
								group by entityid
								Having sum(Convert(INT,statementmonths)) = 24
								)

--select distinct  entityid from #Temp_Quaterly_24
--select * from #Quaterly_Run_24 where entityid = 32850
--order by entityid, ranked_order ASC
-- Remove all entity Ids with Quarerly Financials found to represent latest two years from entity ID list, 
Delete from #Temp_Dist_Ids
where entityid in (select distinct entityid from #Temp_Quaterly_24 group by entityid having count(*) > 1)
------------------------------------**************************************************


----------------------------------********************* Quartley 12 Month Run ******************************

IF OBJECT_ID(N'tempdb..#Quaterly_Run_12') IS NOT NULL
BEGIN
DROP TABLE #Quaterly_Run_12;
END;
--drop table #Monthly_Financials_5Yr_with_Ranked_order_By_statmentdate
	
		select
		distinct
		convert(date,statementdatekey_) AS 'Statment_date',
		statementdatekey_,
		pkid_,
		entityid,
		auditmethod,
		statementmonths,
		statementtype,
		Audit_Rank,
		StatmentType_Rank,
		ROW_NUMBER() OVER (PARTITION BY entityid ORDER BY statementdatekey_ DESC) ranked_order,	
		'' AS 'CF_Type'
		into #Quaterly_Run_12
		From
					(
					select distinct 
					convert(date,statementdatekey_) AS 'Statment_date',
					*
					from #Financials_5Yr
					Where auditmethod in (
											 'Unqualif''d',
											 'Qualified',
											 'Reviewed',
											 'Compiled',
											 'Co.Prep''d'
											) --- apply autitmethod filter          
											and statementtype NOT IN ('FY-To-Date','Rolling Stmt')
											and pkid_
											in(
											select					
												max(pkid_) as pkid_
												from #Financials_5Yr
												Where auditmethod in (
																	 'Unqualif''d',
																	 'Qualified',
																	 'Reviewed',
																	 'Compiled',
																	 'Co.Prep''d'
																	) --- apply autitmethod filter          
																	and statementtype NOT IN ('FY-To-Date','Rolling Stmt')
																	--and entityid = 51350
																	group by entityid, statementdatekey_, statementtype
												)

					) IB
		where entityid IN (Select * from #Temp_Dist_Ids)
		and statementmonths = '3' 

--select * from 	#Quaterly_Run	

-------------------------------------------  Mark latest 24 months worth of data as Monthly using Q for downstream filtering. -------------------------------------------------------

--Check for Annual Statments group by entity id and update Q_Run to Q to represent Annual

DECLARE @entityid5 varchar(20)
DECLARE @statmentdatekey_5 Date 
DECLARE @statementtype5 varchar(20)
DECLARE @statementmonths5 int
DECLARE @Months5 int



DECLARE Ent_Cursor CURSOR FOR
select entityid, statementdatekey_, statementmonths, statementtype from #Quaterly_Run_12

Open Ent_Cursor
Fetch next from Ent_Cursor INTO @entityid5, @statmentdatekey_5, @statementmonths5, @statementtype5

While @@Fetch_Status = 0

BEGIN
	Update #Quaterly_Run_12
	Set CF_Type = 'G'
	where ranked_order < 5 --and entityid = 13320
	--Where statementmonths = 12 and statementdatekey_ >= @Prior_Statment_Date_EndPoint
	--set @Months = Cast(@statementmonths as int)

	--print DATEADD(month, -@Months,  Cast(@statmentdatekey_ as date))



	Fetch Next From Ent_Cursor INTO @entityid5, @statmentdatekey_5, @statementmonths5, @statementtype5
END

CLOSE Ent_Cursor
DEALLOCATE Ent_Cursor



----------------- Select latest 24 months worth of data for each entity where found


IF OBJECT_ID(N'tempdb..#Temp_Quaterly_12') IS NOT NULL
BEGIN
DROP TABLE #Temp_Quaterly_12;
END;
--drop table #Temp_Monthly_24
select 
entityid, 
pkid_, 
statementdatekey_, 
convert(date,statementdatekey_) AS 'Statment_date',
statementtype,
statementmonths,
ranked_order,
--Q_Run,
CF_Type
into #Temp_Quaterly_12
from #Quaterly_Run_12
where CF_Type = 'G' 

				and entityid in (
								select entityid from #Quaterly_Run_12
								where CF_Type = 'G'
								group by entityid
								Having sum(Convert(INT,statementmonths)) = 12
								)

--select distinct  entityid from #Temp_Quaterly_12
--order by entityid, ranked_order ASC
-- Remove all entity Ids with Quarerly Financials found to represent latest two years from entity ID list, 
Delete from #Temp_Dist_Ids
where entityid in (select distinct entityid from #Temp_Quaterly_12 group by entityid having count(*) > 1)
------------------------------------**************************************************
-------------------------------------------***************************************************

------------------------------*********************** Montly Residual Run 1 *******************************

IF OBJECT_ID(N'tempdb..#Residual_Run_1') IS NOT NULL
BEGIN
DROP TABLE #Residual_Run_1;
END;
		select
		distinct
		convert(date,statementdatekey_) AS 'Statment_date',
		statementdatekey_,
		pkid_,
		entityid,
		auditmethod,
		statementmonths,
		statementtype,
		Audit_Rank,
		StatmentType_Rank,
		ROW_NUMBER() OVER (PARTITION BY entityid ORDER BY statementdatekey_ DESC) ranked_order,	
		'Residual' AS 'CF_Type'
		into #Residual_Run_1
		From
					(
					select distinct 
					convert(date,statementdatekey_) AS 'Statment_date',
					*
					from #Financials_5Yr
					Where auditmethod in (
											 'Unqualif''d',
											 'Qualified',
											 'Reviewed',
											 'Compiled',
											 'Co.Prep''d'
											) --- apply autitmethod filter          
											and statementtype NOT IN ('FY-To-Date','Rolling Stmt')
											and pkid_
											in(
											select					
												max(pkid_) as pkid_
												from #Financials_5Yr
												Where auditmethod in (
																	 'Unqualif''d',
																	 'Qualified',
																	 'Reviewed',
																	 'Compiled',
																	 'Co.Prep''d'
																	) --- apply autitmethod filter          
																	and statementtype NOT IN ('FY-To-Date','Rolling Stmt')
																	--and entityid = 51350
																	group by entityid, statementdatekey_,statementtype
												)

					) IB
		where entityid IN (Select * from #Temp_Dist_Ids)
		--and statementmonths = '3' 
----------------------------------------------*************************

--DECLARE @entityid6 varchar(20)
--DECLARE @statmentdatekey_6 Date 
--DECLARE @statementtype6 varchar(20)
--DECLARE @statementmonths6 int
--DECLARE @Months_Count int
--DECLARE @St_Date int
--DECLARE @StartingPoint int
--set @StartingPoint = 1


--DECLARE Ent_Cursor CURSOR FOR
--select entityid, statementdatekey_, statementmonths, statementtype, ranked_order from #Residual_Run_1 where entityid = 42973 order by entityid, ranked_order ASC --IN (50995,42973,50657)

--Open Ent_Cursor
--Fetch next from Ent_Cursor INTO @entityid6, @statmentdatekey_6, @statementmonths6, @statementtype6

--While @@Fetch_Status = 0

--BEGIN
--	--Update #Residual_Run_1
--	--Set Res_Run = 'R'
--	--where ranked_order < 9 --and entityid = 13320
--	--Where statementmonths = 12 and statementdatekey_ >= @Prior_Statment_Date_EndPoint
--	if @StartingPoint = 1
--		update #Residual_Run_1 
--		Set R_Run_12 = 'R'		
--	else if @StartingPoint <> 1 and @statmentdatekey_6 = @St_Date 
--				update #Residual_Run_1 Set R_Run_12 = 'R'
--				set @St_Date = DATEADD(month, -@Months_Count,  Cast(@statmentdatekey_6 as date))
--				Set @StartingPoint = @StartingPoint+1
--	--else
--	--if @Months_Count <> statementdatekey_ 
--	--set @Months_Count = Cast(@statementmonths6 as int)

--	--print DATEADD(month, -@Months_Count,  Cast(@statmentdatekey_6 as date))
--	end
--	Set @StartingPoint = @StartingPoint+1
	 

--	Fetch Next From Ent_Cursor INTO @entityid6, @statmentdatekey_6, @statementmonths6, @statementtype6
--END

--CLOSE Ent_Cursor
--DEALLOCATE Ent_Cursor
------------------------------***********************************************************************************

--select * from #Residual_Run_1 where entityid IN (42973)
--order by entityid, ranked_order ASC




-------------------- Query to see whats lef over

IF OBJECT_ID(N'tempdb..#Residual_Data') IS NOT NULL
BEGIN
DROP TABLE #Residual_Data;
END;
--drop table #Residual_Data
	
		select
		distinct
		convert(date,statementdatekey_) AS 'Statment_date',
		statementdatekey_,
		pkid_,
		entityid,
		auditmethod,
		statementmonths,
		statementtype,
		Audit_Rank,
		StatmentType_Rank,
		ROW_NUMBER() OVER (PARTITION BY entityid ORDER BY statementdatekey_ DESC) ranked_order,
		--'' AS 'A_Run',
		--'' AS 'Q_Run'
		'R' AS CF_Type
		into #Residual_Data
		From
					(
					select distinct 
					convert(date,statementdatekey_) AS 'Statment_date',
					*
					from #Financials_5Yr
					Where auditmethod in (
											 'Unqualif''d',
											 'Qualified',
											 'Reviewed',
											 'Compiled',
											 'Co.Prep''d'
											) --- apply autitmethod filter          
											and statementtype NOT IN ('FY-To-Date','Rolling Stmt')
											and pkid_
											in(
											select					
												max(pkid_) as pkid_
												from #Financials_5Yr
												Where auditmethod in (
																	 'Unqualif''d',
																	 'Qualified',
																	 'Reviewed',
																	 'Compiled',
																	 'Co.Prep''d'
																	) --- apply autitmethod filter          
																	and statementtype NOT IN ('FY-To-Date','Rolling Stmt')
																	--and entityid = 51350
																	group by entityid, statementdatekey_,statementtype
												)

					) IB
		where entityid IN (Select * from #Temp_Dist_Ids)



--- Show Residual
--select distinct entityid from #Residual_Data
--select * from #Temp_Dist_Ids

--- Show Two year Annuals
--select * from #Temp_Annuals_24	
--order by entityid DESC

--Show One Annual
--select * from #Temp_Annuals_12
		
--select distinct entityid from #Temp_Monthly_12
--select distinct entityid from #Temp_Monthly_24
--select distinct entityid from #Temp_Annuals_24
--select distinct entityid  from #Temp_Annuals_12
--select distinct entityid from #Residual_Data



--select * from #Temp_Annuals_24 order by entityid, ranked_order DESC --24 Months
--select * from #Temp_Monthly_24 order by entityid, ranked_order DESC
--select * from #Temp_Annuals_12 order by entityid, ranked_order DESC --12 Months
--select * from #Temp_Monthly_12 order by entityid, ranked_order DESC
--select * from #Temp_Quaterly_24 order by entityid, ranked_order DESC -- 24 Months
--select * from #Temp_Quaterly_12 order by entityid, ranked_order DESC -- 24 Months
--select * from #Residual_Data order by entityid, ranked_order ASC


--Select entityid,pkid_,statementdatekey_,statment_date,statementtype,statementmonths,ranked_order,CF_Type 
--from
--#Temp_Annuals_24 order by entityid, ranked_order DESC


IF OBJECT_ID(N'tempdb..#UnionAllData') IS NOT NULL
BEGIN
DROP TABLE #UnionAllData;
END;
--drop table #Final_5Yr_Financial_Output_Stage
select *
into #UnionAllData
from
(
select entityid,pkid_,statementdatekey_,statment_date,statementtype,statementmonths,ranked_order,CF_Type from #Temp_Annuals_24 -- where ENTITYID IN (34904)
UNION 																															----
Select entityid,pkid_,statementdatekey_,statment_date,statementtype,statementmonths,ranked_order,CF_Type from #Temp_Monthly_24 -- where entityid = 34904
UNION																															----
select entityid,pkid_,statementdatekey_,statment_date,statementtype,statementmonths,ranked_order,CF_Type from #Temp_Annuals_12 -- where ENTITYID IN (34904)
UNION 																															----
Select entityid,pkid_,statementdatekey_,statment_date,statementtype,statementmonths,ranked_order,CF_Type from #Temp_Monthly_12 -- where ENTITYID IN (34904)
UNION																															----
Select entityid,pkid_,statementdatekey_,statment_date,statementtype,statementmonths,ranked_order,CF_Type from #Temp_Quaterly_24--  where entityid = 34904
UNION																															----
select entityid,pkid_,statementdatekey_,statment_date,statementtype,statementmonths,ranked_order,CF_Type from #Temp_Quaterly_12--  where entityid = 34904
UNION 																															----
Select entityid,pkid_,statementdatekey_,statment_date,statementtype,statementmonths,ranked_order,CF_Type from #Residual_Data   -- where entityid = 34904
) as #UnionAllData


---------------------------------------
--start pulling field base on pkid_
--- pulling field base

--select * from #UnionAllData where entityid =  51350

----------------------------------*********************************************************** Start Pulling current data fields for 24 month Annual based on pkid_ 
IF OBJECT_ID(N'tempdb..#Final_CYear_Data') IS NOT NULL
BEGIN
DROP TABLE #Final_CYear_Data;
END;
--drop table #Final_CYear_Data
select 
distinct
C.entityid,
C.statementdatekey_,
C.statementmonths,
C.pkid_,
C.CF_Type,
MS.AOD
into #Final_CYear_Data
from --#Final_5Yr_Financial_Output_Stage F
	(
	select * from #UnionAllData where ranked_order = 1 and CF_Type IN ('A','C')  --and entityid = 13221
	) C
left join
		(
		
		select entityid, statementid,
		AsOfDate as AOD 
		from [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS]		
		) MS
		ON C.pkid_ = Concat(MS.entityid,MS.statementid)
---------------------------------- End Current for 24 months


----------------------------------*********************************************************** Start Pulling prior data fields for 24 month Annual based on pkid_ 
IF OBJECT_ID(N'tempdb..#Final_PYear_Data') IS NOT NULL
BEGIN
DROP TABLE #Final_PYear_Data;
END;
--drop table #Final_CYear_Data
select 
distinct
C.entityid,
C.statementdatekey_,
C.statementmonths,
C.pkid_,
C.CF_Type,
MS.AOD

into #Final_PYear_Data
from --#Final_5Yr_Financial_Output_Stage F
	(
	select * from #UnionAllData where ranked_order = 2 and CF_Type = 'A' --and entityid = 13221
	) C
left join
		(
		
		select entityid, statementid,
		AsOfDate as AOD 
		from [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS]		
		) MS
		ON C.pkid_ = Concat(MS.entityid,MS.statementid)
---------------------------------- End Prior for 24 Months


---------------------------------------------- pull Current Annual Data points 

IF OBJECT_ID(N'tempdb..#CurrentYear_Annual_Data') IS NOT NULL
BEGIN
DROP TABLE #CurrentYear_Annual_Data;
END;
select
distinct 
'C' AS 'TimeFrame',
A.*,
ROUND(isnull(B.netfixedassets,0),0) + ROUND(isnull(B.amortexpense,0),0) + ROUND(isnull(B.deprecexpense,0),0)  as CapitalExpenditures,
             ROUND(isnull(B.Cash,0),0) + ROUND(isnull(B.marketablesec,0),0) as CashMarketableSecurities,
               CASE 
                      WHEN B.cpltd IS NOT NULL 
                          THEN ROUND(ROUND(B.cpltd,0) / ROUND(A.statementmonths,0),0)
                      WHEN B.cpltd IS NULL 
                          THEN B.cpltd 
                              ELSE NULL
                                  END AS CurrentMaturitiesLongTermDebt,
              convert(varchar(8),cast(A.statementdatekey_ as date),112) AS DateOfFinancials,
              ROUND(B.amortexpense, 0) AS DepreciationAmortization,
              CASE 
                      WHEN B.fixedassets IS NOT NULL THEN ROUND(B.fixedassets, 0) 
                      WHEN B.fixedassets IS NULL AND B.netfixedassets IS NOT NULL THEN ROUND(B.netfixedassets, 0) 
                      ELSE NULL
                      END AS FixedAssets,
              ROUND(B.interestexpense, 0) AS InterestExpense,                   
              ROUND(B.longtermdebt, 0) AS LongTermDebt,
			  Case When B.MinorityInterest is null then ROUND(B.minintequity, 0)  
							Else ROUND(B.MinorityInterest,0) end AS MinorityInterest,
             -- B.minorityinterest AS MinorityInterest,
              ROUND(B.netopprofit, 0) AS OperatingIncome,
              ROUND(B.retainedearnings, 0)AS RetainedEarnings,
              ROUND(B.stloanspayable, 0) AS ShortTermDebt,
              ROUND(B.totalnoncurasts,0) + ROUND(B.amortexpense,0) + ROUND(B.intangibles,0) AS TangibleAssets,
COALESCE(CONVERT(NUMERIC,B.totalnoncurliabs),0) + (COALESCE(CONVERT(NUMERIC,B.totalcurliabs), 0)) TotalLiabilities,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS AccountsPayableCurrent,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS [Accounts Payable (A/P) Prior Year],
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS AccountsReceivableCurrent,
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS [Accounts Receivable (A/R) Prior Year],
ROUND(B.totalcurassets, 0) AS CurrentAssetsCurrent,
ROUND(B.totalcurassets, 0) AS [Current Assets Prior Year], 
ROUND(B.totalcurliabs, 0) AS CurrentLiabilitiesCurrent,
ROUND(B.totalcurliabs, 0) AS [Current Liabilities Prior Year],
ROUND(B.totalinventory, 0) AS [Inventory Current],
ROUND(B.totalinventory, 0) AS [Inventory Prior Year],
ROUND(B.netprofit, 0) AS NetIncomeCurrent,
ROUND(B.netprofit, 0) AS [Net Income Prior Year],
ROUND(B.netsales, 0) AS NetSalesCurrent,
ROUND(B.netsales, 0) AS [Net Sales Prior Year],
ROUND(B.totalassets, 0) AS TotalAssetsCurrent,
ROUND(B.totalassets, 0) AS [Total Assets Prior Year]
into #CurrentYear_Annual_Data
from #Final_CYear_Data A
Left join [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS] B
ON A.pkid_ = Concat(B.entityid,B.statementid)


--select * from #CurrentYear_Annual_Data where entityid = 51350

--select * from #UnionAllData where entityid = 51350 order by ranked_order


--#UnionAllData where ENTITYID = 51350

--select minorityinterest,* from [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS] 
--select * from #CurrentYear_Annual_Data

----------------------------------------------Get statmentID for downstream joining

--5135032
--5135023
--5135029
--5135018
--5135021
--5135013
--5135028
--5135027
--5135016
--5135020
--5135019
--5135026
---------------------------------------------- pull Prior Annual Data points 

IF OBJECT_ID(N'tempdb..#PriorYear_Annual_Data') IS NOT NULL
BEGIN
DROP TABLE #PriorYear_Annual_Data;
END;
select
distinct 
'P' AS 'TimeFrame',
A.*,
ROUND(isnull(B.netfixedassets,0),0) + ROUND(isnull(B.amortexpense,0),0) + ROUND(isnull(B.deprecexpense,0),0)  as CapitalExpenditures,
             ROUND(isnull(B.Cash,0),0) + ROUND(isnull(B.marketablesec,0),0) as CashMarketableSecurities,
               CASE 
                      WHEN B.cpltd IS NOT NULL 
                          THEN ROUND(ROUND(B.cpltd,0) / ROUND(A.statementmonths,0),0)
                      WHEN B.cpltd IS NULL 
                          THEN B.cpltd 
                              ELSE NULL
                                  END AS CurrentMaturitiesLongTermDebt,
              convert(varchar(8),cast(A.statementdatekey_ as date),112) AS DateOfFinancials,
              ROUND(B.amortexpense, 0) AS DepreciationAmortization,
              CASE 
                      WHEN B.fixedassets IS NOT NULL THEN ROUND(B.fixedassets, 0) 
                      WHEN B.fixedassets IS NULL AND B.netfixedassets IS NOT NULL THEN ROUND(B.netfixedassets, 0) 
                      ELSE NULL
                      END AS FixedAssets,
              ROUND(B.interestexpense, 0) AS InterestExpense,                   
              ROUND(B.longtermdebt, 0) AS LongTermDebt,
            Case When B.MinorityInterest is null then ROUND(B.minintequity, 0)  
							Else ROUND(B.MinorityInterest,0) end AS MinorityInterest,
              --B.minorityinterest AS MinorityInterest,
              ROUND(B.netopprofit, 0) AS OperatingIncome,
              ROUND(B.retainedearnings, 0)AS RetainedEarnings,
              ROUND(B.stloanspayable, 0) AS ShortTermDebt,
              ROUND(B.totalnoncurasts,0) + ROUND(B.amortexpense,0) + ROUND(B.intangibles,0) AS TangibleAssets,
COALESCE(CONVERT(NUMERIC,B.totalnoncurliabs),0) + (COALESCE(CONVERT(NUMERIC,B.totalcurliabs), 0)) TotalLiabilities,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS AccountsPayableCurrent,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS [Accounts Payable (A/P) Prior Year],
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS AccountsReceivableCurrent,
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS [Accounts Receivable (A/R) Prior Year],
ROUND(B.totalcurassets, 0) AS CurrentAssetsCurrent,
ROUND(B.totalcurassets, 0) AS [Current Assets Prior Year], 
ROUND(B.totalcurliabs, 0) AS CurrentLiabilitiesCurrent,
--ROUND(B.totalcurassets, 0) AS [Current Assets Prior Year], 
ROUND(B.totalcurliabs, 0) AS [Current Liabilities Prior Year],
ROUND(B.totalinventory, 0) AS [Inventory Current],
ROUND(B.totalinventory, 0) AS [Inventory Prior Year],
ROUND(B.netprofit, 0) AS NetIncomeCurrent,
ROUND(B.netprofit, 0) AS [Net Income Prior Year],
ROUND(B.netsales, 0) AS NetSalesCurrent,
ROUND(B.netsales, 0) AS [Net Sales Prior Year],
ROUND(B.totalassets, 0) AS TotalAssetsCurrent,
ROUND(B.totalassets, 0) AS [Total Assets Prior Year]
into #PriorYear_Annual_Data
from #Final_PYear_Data A
Left join [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS] B
ON A.pkid_ = Concat(B.entityid,B.statementid)
------------------------------------------------------------------------ End


----------------------------------------------------------- Get 24 month Monthly data points
IF OBJECT_ID(N'tempdb..#Final_CMYear_Data') IS NOT NULL
BEGIN
DROP TABLE #Final_CMYear_Data;
END;
--drop table #Final_CYear_Data
select 
distinct
C.entityid,
C.statementdatekey_,
C.statementmonths,
C.pkid_,
C.CF_Type,
MS.AOD
into #Final_CMYear_Data
from --#Final_5Yr_Financial_Output_Stage F
	(
	select * from #UnionAllData where ranked_order <= 12 and CF_Type IN ('B') -- and entityid = 13320
	) C
left join
		(
		
		select entityid, statementid,
		AsOfDate as AOD 
		from [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS]		
		) MS
		ON C.pkid_ = Concat(MS.entityid,MS.statementid)
--select * from #PriorYear_Annual_Data

--select * from #CurrentYear_Annual_Data where entityid = 13221
--select * from #PriorYear_Annual_Data where entityid = 13221

----------------------------------------------Get statmentID for downstream joining

---------------------------------- Pull Current 12 Month Montly data points
IF OBJECT_ID(N'tempdb..#Current_CM_Year_Data') IS NOT NULL
BEGIN
DROP TABLE #Current_CM_Year_Data;
END;
IF OBJECT_ID(N'tempdb..#CurrentYear_Month_Monthly_Data') IS NOT NULL
BEGIN
DROP TABLE #CurrentYear_Month_Monthly_Data;
END;
select
distinct 
'C' AS 'TimeFrame',
A.*,
ROUND(isnull(B.netfixedassets,0),0) + ROUND(isnull(B.amortexpense,0),0) + ROUND(isnull(B.deprecexpense,0),0)  as CapitalExpenditures,
             ROUND(isnull(B.Cash,0),0) + ROUND(isnull(B.marketablesec,0),0) as CashMarketableSecurities,
               CASE 
                      WHEN B.cpltd IS NOT NULL 
                          THEN ROUND(ROUND(B.cpltd,0) / ROUND(A.statementmonths,0),0)
                      WHEN B.cpltd IS NULL 
                          THEN B.cpltd 
                              ELSE NULL
                                  END AS CurrentMaturitiesLongTermDebt,
              convert(varchar(8),cast(A.statementdatekey_ as date),112) AS DateOfFinancials,
              ROUND(B.amortexpense, 0) AS DepreciationAmortization,
              CASE 
                      WHEN B.fixedassets IS NOT NULL THEN ROUND(B.fixedassets, 0) 
                      WHEN B.fixedassets IS NULL AND B.netfixedassets IS NOT NULL THEN ROUND(B.netfixedassets, 0) 
                      ELSE NULL
                      END AS FixedAssets,
              ROUND(B.interestexpense, 0) AS InterestExpense,                   
              ROUND(B.longtermdebt, 0) AS LongTermDebt,
           Case When B.MinorityInterest is null then ROUND(B.minintequity, 0)  
							Else ROUND(B.MinorityInterest,0) end AS MinorityInterest,
              --B.minorityinterest AS MinorityInterest,
              ROUND(B.netopprofit, 0) AS OperatingIncome,
              ROUND(B.retainedearnings, 0)AS RetainedEarnings,
              ROUND(B.stloanspayable, 0) AS ShortTermDebt,
              ROUND(B.totalnoncurasts,0) + ROUND(B.amortexpense,0) + ROUND(B.intangibles,0) AS TangibleAssets,
COALESCE(CONVERT(NUMERIC,B.totalnoncurliabs),0) + (COALESCE(CONVERT(NUMERIC,B.totalcurliabs), 0)) TotalLiabilities,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS AccountsPayableCurrent,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS [Accounts Payable (A/P) Prior Year],
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS AccountsReceivableCurrent,
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS [Accounts Receivable (A/R) Prior Year],
ROUND(B.totalcurassets, 0) AS CurrentAssetsCurrent,
ROUND(B.totalcurassets, 0) AS [Current Assets Prior Year], 
ROUND(B.totalcurliabs, 0) AS CurrentLiabilitiesCurrent,
--ROUND(B.totalcurassets, 0) AS [Current Assets Prior Year], 
ROUND(B.totalcurliabs, 0) AS [Current Liabilities Prior Year],
ROUND(B.totalinventory, 0) AS [Inventory Current],
ROUND(B.totalinventory, 0) AS [Inventory Prior Year],
ROUND(B.netprofit, 0) AS NetIncomeCurrent,
ROUND(B.netprofit, 0) AS [Net Income Prior Year],
ROUND(B.netsales, 0) AS NetSalesCurrent,
ROUND(B.netsales, 0) AS [Net Sales Prior Year],
ROUND(B.totalassets, 0) AS TotalAssetsCurrent,
ROUND(B.totalassets, 0) AS [Total Assets Prior Year]
into #CurrentYear_Month_Monthly_Data
from #Final_CMYear_Data A
Left join [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS] B
ON A.pkid_ = Concat(B.entityid,B.statementid)
--group by A.ENTITYID, A.STATEMENTDATEKEY_,A.STATEMENTMONTHS,A.AOD


IF OBJECT_ID(N'tempdb..#Final_PMYear_Data') IS NOT NULL
BEGIN
DROP TABLE #Final_PMYear_Data;
END;
--drop table #Final_CYear_Data
select 
distinct
C.entityid,
C.statementdatekey_,
C.statementmonths,
C.pkid_,
C.CF_Type,
MS.AOD
into #Final_PMYear_Data
from --#Final_5Yr_Financial_Output_Stage F
	(
	select * from #UnionAllData where ranked_order > 12 and CF_Type IN ('B') -- and entityid = 13320
	) C
left join
		(
		
		select entityid, statementid,
		AsOfDate as AOD 
		from [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS]		
		) MS
		ON C.pkid_ = Concat(MS.entityid,MS.statementid)
--select * from #PriorYear_Annual_Data

--select * from #CurrentYear_Annual_Data 
--select * from #PriorYear_Annual_Data 

----------------------------------------------Get statmentID for downstream joining

---------------------------------- Pull Current 12 Month Montly data points
IF OBJECT_ID(N'tempdb..#Current_PM_Year_Data') IS NOT NULL
BEGIN
DROP TABLE #Current_PM_Year_Data;
END;
IF OBJECT_ID(N'tempdb..#PriorYear_Month_Monthly_Data') IS NOT NULL
BEGIN
DROP TABLE #PriorYear_Month_Monthly_Data;
END;
select
distinct 
'P' AS 'TimeFrame',
A.*,
ROUND(isnull(B.netfixedassets,0),0) + ROUND(isnull(B.amortexpense,0),0) + ROUND(isnull(B.deprecexpense,0),0)  as CapitalExpenditures,
             ROUND(isnull(B.Cash,0),0) + ROUND(isnull(B.marketablesec,0),0) as CashMarketableSecurities,
               CASE 
                      WHEN B.cpltd IS NOT NULL 
                          THEN ROUND(ROUND(B.cpltd,0) / ROUND(A.statementmonths,0),0)
                      WHEN B.cpltd IS NULL 
                          THEN B.cpltd 
                              ELSE NULL
                                  END AS CurrentMaturitiesLongTermDebt,
              convert(varchar(8),cast(A.statementdatekey_ as date),112) AS DateOfFinancials,
              ROUND(B.amortexpense, 0) AS DepreciationAmortization,
              CASE 
                      WHEN B.fixedassets IS NOT NULL THEN ROUND(B.fixedassets, 0) 
                      WHEN B.fixedassets IS NULL AND B.netfixedassets IS NOT NULL THEN ROUND(B.netfixedassets, 0) 
                      ELSE NULL
                      END AS FixedAssets,
              ROUND(B.interestexpense, 0) AS InterestExpense,                   
              ROUND(B.longtermdebt, 0) AS LongTermDebt,
             Case When B.MinorityInterest is null then ROUND(B.minintequity, 0)  
							Else ROUND(B.MinorityInterest,0) end AS MinorityInterest,
              --B.minorityinterest AS MinorityInterest,
              ROUND(B.netopprofit, 0) AS OperatingIncome,
              ROUND(B.retainedearnings, 0)AS RetainedEarnings,
              ROUND(B.stloanspayable, 0) AS ShortTermDebt,
              ROUND(B.totalnoncurasts,0) + ROUND(B.amortexpense,0) + ROUND(B.intangibles,0) AS TangibleAssets,
COALESCE(CONVERT(NUMERIC,B.totalnoncurliabs),0) + (COALESCE(CONVERT(NUMERIC,B.totalcurliabs), 0)) TotalLiabilities,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS AccountsPayableCurrent,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS [Accounts Payable (A/P) Prior Year],
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS AccountsReceivableCurrent,
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS [Accounts Receivable (A/R) Prior Year],
ROUND(B.totalcurassets, 0) AS CurrentAssetsCurrent,
ROUND(B.totalcurassets, 0) AS [Current Assets Prior Year], 
ROUND(B.totalcurliabs, 0) AS CurrentLiabilitiesCurrent,
--ROUND(B.totalcurassets, 0) AS [Current Assets Prior Year], 
ROUND(B.totalcurliabs, 0) AS [Current Liabilities Prior Year],
ROUND(B.totalinventory, 0) AS [Inventory Current],
ROUND(B.totalinventory, 0) AS [Inventory Prior Year],
ROUND(B.netprofit, 0) AS NetIncomeCurrent,
ROUND(B.netprofit, 0) AS [Net Income Prior Year],
ROUND(B.netsales, 0) AS NetSalesCurrent,
ROUND(B.netsales, 0) AS [Net Sales Prior Year],
ROUND(B.totalassets, 0) AS TotalAssetsCurrent,
ROUND(B.totalassets, 0) AS [Total Assets Prior Year]
into #PriorYear_Month_Monthly_Data
from #Final_PMYear_Data A
Left join [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS] B
ON A.pkid_ = Concat(B.entityid,B.statementid)
--group by A.ENTITYID, A.STATEMENTDATEKEY_,A.STATEMENTMONTHS,A.AOD
----------------------------------------------------------------------------------------


------------------------------------ Get 12 Month Monthly data finding only 1 Year Worth of Data
IF OBJECT_ID(N'tempdb..#Final_CMMYear_Data') IS NOT NULL
BEGIN
DROP TABLE #Final_CMMYear_Data;
END;
--drop table #Final_CYear_Data
select 
distinct
C.entityid,
C.statementdatekey_,
C.statementmonths,
C.pkid_,
C.CF_Type,
MS.AOD
into #Final_CMMYear_Data
from --#Final_5Yr_Financial_Output_Stage F
	(
	select * from #UnionAllData where CF_Type IN ('D') -- and entityid = 13320
	) C
left join
		(
		
		select entityid, statementid,
		AsOfDate as AOD 
		from [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS]		
		) MS
		ON C.pkid_ = Concat(MS.entityid,MS.statementid)
--select * from #PriorYear_Annual_Data

--select * from #CurrentYear_Annual_Data where entityid = 13221
--select * from #PriorYear_Annual_Data where entityid = 13221

----------------------------------------------Get statmentID for downstream joining

---------------------------------- Pull Current 12 Month Montly data points
IF OBJECT_ID(N'tempdb..#Current_CMM_Year_Data') IS NOT NULL
BEGIN
DROP TABLE #Current_CMM_Year_Data;
END;
IF OBJECT_ID(N'tempdb..#CurrentYear_Month_Monthly_Only_One_Year_Worth_Data') IS NOT NULL
BEGIN
DROP TABLE #CurrentYear_Month_Monthly_Only_One_Year_Worth_Data;
END;
select
distinct 
'C' AS 'TimeFrame',
A.*,
ROUND(isnull(B.netfixedassets,0),0) + ROUND(isnull(B.amortexpense,0),0) + ROUND(isnull(B.deprecexpense,0),0)  as CapitalExpenditures,
             ROUND(isnull(B.Cash,0),0) + ROUND(isnull(B.marketablesec,0),0) as CashMarketableSecurities,
               CASE 
                      WHEN B.cpltd IS NOT NULL 
                          THEN ROUND(ROUND(B.cpltd,0) / ROUND(A.statementmonths,0),0)
                      WHEN B.cpltd IS NULL 
                          THEN B.cpltd 
                              ELSE NULL
                                  END AS CurrentMaturitiesLongTermDebt,
              convert(varchar(8),cast(A.statementdatekey_ as date),112) AS DateOfFinancials,
              ROUND(B.amortexpense, 0) AS DepreciationAmortization,
              CASE 
                      WHEN B.fixedassets IS NOT NULL THEN ROUND(B.fixedassets, 0) 
                      WHEN B.fixedassets IS NULL AND B.netfixedassets IS NOT NULL THEN ROUND(B.netfixedassets, 0) 
                      ELSE NULL
                      END AS FixedAssets,
              ROUND(B.interestexpense, 0) AS InterestExpense,                   
              ROUND(B.longtermdebt, 0) AS LongTermDebt,
            Case When B.MinorityInterest is null then ROUND(B.minintequity, 0)  
							Else ROUND(B.MinorityInterest,0) end AS MinorityInterest,
              --B.minorityinterest AS MinorityInterest,
              ROUND(B.netopprofit, 0) AS OperatingIncome,
              ROUND(B.retainedearnings, 0)AS RetainedEarnings,
              ROUND(B.stloanspayable, 0) AS ShortTermDebt,
              ROUND(B.totalnoncurasts,0) + ROUND(B.amortexpense,0) + ROUND(B.intangibles,0) AS TangibleAssets,
COALESCE(CONVERT(NUMERIC,B.totalnoncurliabs),0) + (COALESCE(CONVERT(NUMERIC,B.totalcurliabs), 0)) TotalLiabilities,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS AccountsPayableCurrent,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS [Accounts Payable (A/P) Prior Year],
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS AccountsReceivableCurrent,
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS [Accounts Receivable (A/R) Prior Year],
ROUND(B.totalcurassets, 0) AS CurrentAssetsCurrent,
ROUND(B.totalcurassets, 0) AS [Current Assets Prior Year], 
ROUND(B.totalcurliabs, 0) AS CurrentLiabilitiesCurrent,
--ROUND(B.totalcurassets, 0) AS [Current Assets Prior Year], 
ROUND(B.totalcurliabs, 0) AS [Current Liabilities Prior Year],
ROUND(B.totalinventory, 0) AS [Inventory Current],
ROUND(B.totalinventory, 0) AS [Inventory Prior Year],
ROUND(B.netprofit, 0) AS NetIncomeCurrent,
ROUND(B.netprofit, 0) AS [Net Income Prior Year],
ROUND(B.netsales, 0) AS NetSalesCurrent,
ROUND(B.netsales, 0) AS [Net Sales Prior Year],
ROUND(B.totalassets, 0) AS TotalAssetsCurrent,
ROUND(B.totalassets, 0) AS [Total Assets Prior Year]
into #CurrentYear_Month_Monthly_Only_One_Year_Worth_Data
from #Final_CMMYear_Data A
Left join [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS] B
ON A.pkid_ = Concat(B.entityid,B.statementid)
--group by A.ENTITYID, A.STATEMENTDATEKEY_,A.STATEMENTMONTHS,A.AOD
----------------------------------------------------------------------------------


------------------------------------ Get 12 Month Monthly data finding only 1 Year Worth of Data
IF OBJECT_ID(N'tempdb..#Final_CQYear_Data') IS NOT NULL
BEGIN
DROP TABLE #Final_CQYear_Data;
END;
--drop table #Final_CYear_Data
select 
distinct
C.entityid,
C.statementdatekey_,
C.statementmonths,
C.pkid_,
C.CF_Type,
MS.AOD
into #Final_CQYear_Data
from --#Final_5Yr_Financial_Output_Stage F
	(
	select * from #UnionAllData where ranked_order <= 4 and CF_Type IN ('F') -- and entityid = 13320
	) C
left join
		(
		
		select entityid, statementid,
		AsOfDate as AOD 
		from [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS]		
		) MS
		ON C.pkid_ = Concat(MS.entityid,MS.statementid)


IF OBJECT_ID(N'tempdb..#Final_CQQYear_Data') IS NOT NULL
BEGIN
DROP TABLE #Final_CQQYear_Data;
END;
--drop table #Final_CYear_Data
select 
distinct
C.entityid,
C.statementdatekey_,
C.statementmonths,
C.pkid_,
C.CF_Type,
MS.AOD
into #Final_CQQYear_Data
from --#Final_5Yr_Financial_Output_Stage F
	(
	select * from #UnionAllData where ranked_order > 4 and CF_Type IN ('F') -- and entityid = 13320
	) C
left join
		(
		
		select entityid, statementid,
		AsOfDate as AOD 
		from [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS]		
		) MS
		ON C.pkid_ = Concat(MS.entityid,MS.statementid)
----------------------------------------------Get statmentID for downstream joining

---------------------------------- Pull Current 12 Month Montly data points
IF OBJECT_ID(N'tempdb..#Current_CQ_Year_Data') IS NOT NULL
BEGIN
DROP TABLE #Current_CQ_Year_Data;
END;
IF OBJECT_ID(N'tempdb..#CurrentYear_Quaterly_Data') IS NOT NULL
BEGIN
DROP TABLE #CurrentYear_Quaterly_Data;
END;
select
distinct 
'C' AS 'TimeFrame',
A.*,
ROUND(isnull(B.netfixedassets,0),0) + ROUND(isnull(B.amortexpense,0),0) + ROUND(isnull(B.deprecexpense,0),0)  as CapitalExpenditures,
             ROUND(isnull(B.Cash,0),0) + ROUND(isnull(B.marketablesec,0),0) as CashMarketableSecurities,
               CASE 
                      WHEN B.cpltd IS NOT NULL 
                          THEN ROUND(ROUND(B.cpltd,0) / ROUND(A.statementmonths,0),0)
                      WHEN B.cpltd IS NULL 
                          THEN B.cpltd 
                              ELSE NULL
                                  END AS CurrentMaturitiesLongTermDebt,
              convert(varchar(8),cast(A.statementdatekey_ as date),112) AS DateOfFinancials,
              ROUND(B.amortexpense, 0) AS DepreciationAmortization,
              CASE 
                      WHEN B.fixedassets IS NOT NULL THEN ROUND(B.fixedassets, 0) 
                      WHEN B.fixedassets IS NULL AND B.netfixedassets IS NOT NULL THEN ROUND(B.netfixedassets, 0) 
                      ELSE NULL
                      END AS FixedAssets,
              ROUND(B.interestexpense, 0) AS InterestExpense,                   
              ROUND(B.longtermdebt, 0) AS LongTermDebt,
              Case When B.MinorityInterest is null then ROUND(B.minintequity, 0)  
							Else ROUND(B.MinorityInterest,0) end AS MinorityInterest,
              --B.minorityinterest AS MinorityInterest,
              ROUND(B.netopprofit, 0) AS OperatingIncome,
              ROUND(B.retainedearnings, 0)AS RetainedEarnings,
              ROUND(B.stloanspayable, 0) AS ShortTermDebt,
              ROUND(B.totalnoncurasts,0) + ROUND(B.amortexpense,0) + ROUND(B.intangibles,0) AS TangibleAssets,
COALESCE(CONVERT(NUMERIC,B.totalnoncurliabs),0) + (COALESCE(CONVERT(NUMERIC,B.totalcurliabs), 0)) TotalLiabilities,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS AccountsPayableCurrent,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS [Accounts Payable (A/P) Prior Year],
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS AccountsReceivableCurrent,
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS [Accounts Receivable (A/R) Prior Year],
ROUND(B.totalcurassets, 0) AS CurrentAssetsCurrent,
ROUND(B.totalcurassets, 0) AS [Current Assets Prior Year], 
ROUND(B.totalcurliabs, 0) AS CurrentLiabilitiesCurrent,
--ROUND(B.totalcurassets, 0) AS [Current Assets Prior Year], 
ROUND(B.totalcurliabs, 0) AS [Current Liabilities Prior Year],
ROUND(B.totalinventory, 0) AS [Inventory Current],
ROUND(B.totalinventory, 0) AS [Inventory Prior Year],
ROUND(B.netprofit, 0) AS NetIncomeCurrent,
ROUND(B.netprofit, 0) AS [Net Income Prior Year],
ROUND(B.netsales, 0) AS NetSalesCurrent,
ROUND(B.netsales, 0) AS [Net Sales Prior Year],
ROUND(B.totalassets, 0) AS TotalAssetsCurrent,
ROUND(B.totalassets, 0) AS [Total Assets Prior Year]
into #CurrentYear_Quaterly_Data
from #Final_CQYear_Data A
Left join [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS] B
ON A.pkid_ = Concat(B.entityid,B.statementid)
--group by A.ENTITYID, A.STATEMENTDATEKEY_,A.STATEMENTMONTHS,A.AOD


IF OBJECT_ID(N'tempdb..#Current_PQ_Year_Data') IS NOT NULL
BEGIN
DROP TABLE #Current_PQ_Year_Data;
END;
IF OBJECT_ID(N'tempdb..#PriorYear_Quaterly_Data') IS NOT NULL
BEGIN
DROP TABLE #PriorYear_Quaterly_Data;
END;
select
distinct 
'P' AS 'TimeFrame',
A.*,
ROUND(isnull(B.netfixedassets,0),0) + ROUND(isnull(B.amortexpense,0),0) + ROUND(isnull(B.deprecexpense,0),0)  as CapitalExpenditures,
             ROUND(isnull(B.Cash,0),0) + ROUND(isnull(B.marketablesec,0),0) as CashMarketableSecurities,
               CASE 
                      WHEN B.cpltd IS NOT NULL 
                          THEN ROUND(ROUND(B.cpltd,0) / ROUND(A.statementmonths,0),0)
                      WHEN B.cpltd IS NULL 
                          THEN B.cpltd 
                              ELSE NULL
                                  END AS CurrentMaturitiesLongTermDebt,
              convert(varchar(8),cast(A.statementdatekey_ as date),112) AS DateOfFinancials,
              ROUND(B.amortexpense, 0) AS DepreciationAmortization,
              CASE 
                      WHEN B.fixedassets IS NOT NULL THEN ROUND(B.fixedassets, 0) 
                      WHEN B.fixedassets IS NULL AND B.netfixedassets IS NOT NULL THEN ROUND(B.netfixedassets, 0) 
                      ELSE NULL
                      END AS FixedAssets,
              ROUND(B.interestexpense, 0) AS InterestExpense,                   
              ROUND(B.longtermdebt, 0) AS LongTermDebt,
           Case When B.MinorityInterest is null then ROUND(B.minintequity, 0)  
							Else ROUND(B.MinorityInterest,0) end AS MinorityInterest,
             -- B.minorityinterest AS MinorityInterest,
              ROUND(B.netopprofit, 0) AS OperatingIncome,
              ROUND(B.retainedearnings, 0)AS RetainedEarnings,
              ROUND(B.stloanspayable, 0) AS ShortTermDebt,
              ROUND(B.totalnoncurasts,0) + ROUND(B.amortexpense,0) + ROUND(B.intangibles,0) AS TangibleAssets,
COALESCE(CONVERT(NUMERIC,B.totalnoncurliabs),0) + (COALESCE(CONVERT(NUMERIC,B.totalcurliabs), 0)) TotalLiabilities,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS AccountsPayableCurrent,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS [Accounts Payable (A/P) Prior Year],
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS AccountsReceivableCurrent,
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS [Accounts Receivable (A/R) Prior Year],
ROUND(B.totalcurassets, 0) AS CurrentAssetsCurrent,
ROUND(B.totalcurassets, 0) AS [Current Assets Prior Year], 
ROUND(B.totalcurliabs, 0) AS CurrentLiabilitiesCurrent,
--ROUND(B.totalcurassets, 0) AS [Current Assets Prior Year], 
ROUND(B.totalcurliabs, 0) AS [Current Liabilities Prior Year],
ROUND(B.totalinventory, 0) AS [Inventory Current],
ROUND(B.totalinventory, 0) AS [Inventory Prior Year],
ROUND(B.netprofit, 0) AS NetIncomeCurrent,
ROUND(B.netprofit, 0) AS [Net Income Prior Year],
ROUND(B.netsales, 0) AS NetSalesCurrent,
ROUND(B.netsales, 0) AS [Net Sales Prior Year],
ROUND(B.totalassets, 0) AS TotalAssetsCurrent,
ROUND(B.totalassets, 0) AS [Total Assets Prior Year]
into #PriorYear_Quaterly_Data
from #Final_CQQYear_Data A
Left join [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS] B
ON A.pkid_ = Concat(B.entityid,B.statementid)



------------------------------------ Get 12 month Quaterly data finding only 1 Year Worth of Data
IF OBJECT_ID(N'tempdb..#Final_CMQYear_Data') IS NOT NULL
BEGIN
DROP TABLE #Final_CMQYear_Data;
END;
--drop table #Final_CYear_Data
select 
distinct
C.entityid,
C.statementdatekey_,
C.statementmonths,
C.pkid_,
C.CF_Type,
MS.AOD
into #Final_CMQYear_Data
from --#Final_5Yr_Financial_Output_Stage F
	(
	select * from #UnionAllData where CF_Type IN ('G') -- and entityid = 13320
	) C
left join
		(
		
		select entityid, statementid,
		AsOfDate as AOD 
		from [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS]		
		) MS
		ON C.pkid_ = Concat(MS.entityid,MS.statementid)
--select * from #PriorYear_Annual_Data

--select * from #CurrentYear_Annual_Data where entityid = 13221
--select * from #PriorYear_Annual_Data where entityid = 13221

----------------------------------------------Get statmentID for downstream joining

---------------------------------- Pull Current 12 Month Montly data points
IF OBJECT_ID(N'tempdb..#Current_CMQ_Year_Data') IS NOT NULL
BEGIN
DROP TABLE #Current_CMQ_Year_Data;
END;
IF OBJECT_ID(N'tempdb..#CurrentYear_Month_Quaterly_Only_One_Year_Worth_Data') IS NOT NULL
BEGIN
DROP TABLE #CurrentYear_Month_Quaterly_Only_One_Year_Worth_Data;
END;
select
distinct 
'C' AS 'TimeFrame',
A.*,
ROUND(isnull(B.netfixedassets,0),0) + ROUND(isnull(B.amortexpense,0),0) + ROUND(isnull(B.deprecexpense,0),0)  as CapitalExpenditures,
             ROUND(isnull(B.Cash,0),0) + ROUND(isnull(B.marketablesec,0),0) as CashMarketableSecurities,
               CASE 
                      WHEN B.cpltd IS NOT NULL 
                          THEN ROUND(ROUND(B.cpltd,0) / ROUND(A.statementmonths,0),0)
                      WHEN B.cpltd IS NULL 
                          THEN B.cpltd 
                              ELSE NULL
                                  END AS CurrentMaturitiesLongTermDebt,
              convert(varchar(8),cast(A.statementdatekey_ as date),112) AS DateOfFinancials,
              ROUND(B.amortexpense, 0) AS DepreciationAmortization,
              CASE 
                      WHEN B.fixedassets IS NOT NULL THEN ROUND(B.fixedassets, 0) 
                      WHEN B.fixedassets IS NULL AND B.netfixedassets IS NOT NULL THEN ROUND(B.netfixedassets, 0) 
                      ELSE NULL
                      END AS FixedAssets,
              ROUND(B.interestexpense, 0) AS InterestExpense,                   
              ROUND(B.longtermdebt, 0) AS LongTermDebt,
              Case When B.MinorityInterest is null then ROUND(B.minintequity, 0)  
							Else ROUND(B.MinorityInterest,0) end AS MinorityInterest,
             -- B.minorityinterest AS MinorityInterest,
              ROUND(B.netopprofit, 0) AS OperatingIncome,
              ROUND(B.retainedearnings, 0)AS RetainedEarnings,
              ROUND(B.stloanspayable, 0) AS ShortTermDebt,
              ROUND(B.totalnoncurasts,0) + ROUND(B.amortexpense,0) + ROUND(B.intangibles,0) AS TangibleAssets,
COALESCE(CONVERT(NUMERIC,B.totalnoncurliabs),0) + (COALESCE(CONVERT(NUMERIC,B.totalcurliabs), 0)) TotalLiabilities,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS AccountsPayableCurrent,
--ROUND(B.totalassets, 0) AS TotalAssetsCurrent, 
ROUND(B.acctspaytrade, 0) AS [Accounts Payable (A/P) Prior Year],
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS AccountsReceivableCurrent,
(ROUND(COALESCE(CONVERT(numeric,B.acctsrectrade),0) - COALESCE(convert(numeric, B.baddebtreserve),0),0)) AS [Accounts Receivable (A/R) Prior Year],
ROUND(B.totalcurassets, 0) AS CurrentAssetsCurrent,
ROUND(B.totalcurassets, 0) AS [Current Assets Prior Year], 
ROUND(B.totalcurliabs, 0) AS CurrentLiabilitiesCurrent,
--ROUND(B.totalcurassets, 0) AS [Current Assets Prior Year], 
ROUND(B.totalcurliabs, 0) AS [Current Liabilities Prior Year],
ROUND(B.totalinventory, 0) AS [Inventory Current],
ROUND(B.totalinventory, 0) AS [Inventory Prior Year],
ROUND(B.netprofit, 0) AS NetIncomeCurrent,
ROUND(B.netprofit, 0) AS [Net Income Prior Year],
ROUND(B.netsales, 0) AS NetSalesCurrent,
ROUND(B.netsales, 0) AS [Net Sales Prior Year],
ROUND(B.totalassets, 0) AS TotalAssetsCurrent,
ROUND(B.totalassets, 0) AS [Total Assets Prior Year]
into #CurrentYear_Month_Quaterly_Only_One_Year_Worth_Data
from #Final_CMQYear_Data A
Left join [CreditLens].[STG_SVB_UpHiststmtFinancialMMAS] B
ON A.pkid_ = Concat(B.entityid,B.statementid)
--group by A.ENTITYID, A.STATEMENTDATEKEY_,A.STATEMENTMONTHS,A.AOD
----------------------------------------------------------------------------------


IF OBJECT_ID(N'tempdb..#AllData_Final_Stage') IS NOT NULL
BEGIN
DROP TABLE #AllData_Final_Stage;
END;
--drop table #Final_5Yr_Financial_Output_Stage
select *
into #AllData_Final_Stage
from
(
select * from #CurrentYear_Annual_Data
UNION
select * from #PriorYear_Annual_Data
UNION 								
select * from #CurrentYear_Month_Monthly_Data
UNION 			
select * from #PriorYear_Month_Monthly_Data
UNION 			
select * from #CurrentYear_Month_Monthly_Only_One_Year_Worth_Data
UNION 			
select * from #CurrentYear_Quaterly_Data
UNION 			
select * from #PriorYear_Quaterly_Data
UNION 			
select * from #CurrentYear_Month_Quaterly_Only_One_Year_Worth_Data
) as #AllData_Final_Stage

--select distinct CF_Type from #AllData_Final_Stage
--select * from #AllData_Final_Stage
--group by entityid 







--select 
--entityid,
--Sum(CapitalExpenditures) AS CapitalExpenditures,
--Sum(CashMarketableSecurities) AS CashMarketableSecurities,
--Sum(CurrentMaturitiesLongTermDebt) AS CurrentMaturitiesLongTermDebt,
--DateOfFinancials,
--Sum(DepreciationAmortization) AS DepreciationAmortization,
--Sum(FixedAssets) AS FixedAssets,
--Sum(InterestExpense) AS InterestExpense,
--Sum(LongTermDebt) AS LongTermDebt,
----MinorityInterest,
-- --MinorityInterest,
--Sum(OperatingIncome) AS OperatingIncome,
--Sum(RetainedEarnings) AS RetainedEarnings,
--Sum(ShortTermDebt) AS ShortTermDebt,
--Sum(TangibleAssets) AS TangibleAssets,
--Sum(TotalLiabilities) AS TotalLiabilities,
--Sum(AccountsPayableCurrent) AS AccountsPayableCurrent,
--Sum([Accounts Payable (A/P) Prior Year]) AS [Accounts Payable (A/P) Prior Year],
--Sum(AccountsReceivableCurrent) AS AccountsReceivableCurrent,
--Sum([Accounts Receivable (A/R) Prior Year]) AS [Accounts Receivable (A/R) Prior Year],
--Sum(CurrentAssetsCurrent) AS CurrentAssetsCurrent,
--Sum([Current Assets Prior Year]) AS [Current Assets Prior Year],
--Sum(CurrentLiabilitiesCurrent) AS CurrentLiabilitiesCurrent,
--Sum(NetIncomeCurrent) AS NetIncomeCurrent,
--Sum([Net Income Prior Year]) AS [Net Income Prior Year],
--Sum(NetSalesCurrent) AS NetSalesCurrent,
--Sum([Net Sales Prior Year]) AS [Net Sales Prior Year],
--Sum(TotalAssetsCurrent) AS TotalAssetsCurrent,
--Sum([Total Assets Prior Year]) AS [Total Assets Prior Year]
--from #CurrentYear_Month_Monthly_Data
----select NULLIF(convert(numeric,MinorityInterest),0) AS MinorityInterest,* from #CurrentYear_Month_Monthly_Data
-- group by entityid
 ------------------------------------------------------

 

IF OBJECT_ID(N'tempdb..#AllData_Final_Stage_Current') IS NOT NULL
BEGIN
DROP TABLE #AllData_Final_Stage_Current;
END;
IF OBJECT_ID(N'tempdb..#AllData_Final_Stage_Prior') IS NOT NULL
BEGIN
DROP TABLE #AllData_Final_Stage_Prior;
END;

select *
into #AllData_Final_Stage_Current
from #AllData_Final_Stage
where TimeFrame = 'C'

select *
into #AllData_Final_Stage_Prior
from #AllData_Final_Stage
where TimeFrame = 'P'

--select * from #AllData_Final_Stage_Current

IF OBJECT_ID(N'tempdb..#AllData_IPE_C') IS NOT NULL
BEGIN
DROP TABLE #AllData_IPE_C;
END;
select 
C.entityid,
Sum(CAST(C.CapitalExpenditures AS numeric)) AS CapitalExpenditures,
Sum(C.CashMarketableSecurities) AS CashMarketableSecurities,
Sum(C.CurrentMaturitiesLongTermDebt) AS CurrentMaturitiesLongTermDebt,
--DateOfFinancials,
Sum(C.DepreciationAmortization) AS DepreciationAmortization,
Sum(C.FixedAssets) AS FixedAssets,
Sum(C.InterestExpense) AS InterestExpense,
Sum(C.LongTermDebt) AS LongTermDebt,
Sum(C.MinorityInterest) AS MinorityInterest,
Sum(C.OperatingIncome) AS OperatingIncome,
Sum(C.RetainedEarnings) AS RetainedEarnings,
Sum(C.ShortTermDebt) AS ShortTermDebt,
Sum(C.TangibleAssets) AS TangibleAssets,
Sum(C.TotalLiabilities) AS TotalLiabilities,
Sum(C.AccountsPayableCurrent) AS AccountsPayableCurrent,
--Sum(P.[Accounts Payable (A/P) Prior Year]) AS [Accounts Payable (A/P) Prior Year],
Sum(C.AccountsReceivableCurrent) AS AccountsReceivableCurrent,
Sum(C.[Inventory Current]) AS [Inventory Current],
--Sum(P.[Accounts Receivable (A/R) Prior Year]) AS [Accounts Receivable (A/R) Prior Year],
Sum(C.CurrentAssetsCurrent) AS CurrentAssetsCurrent,
--Sum(P.[Current Assets Prior Year]) AS [Current Assets Prior Year],
Sum(C.CurrentLiabilitiesCurrent) AS CurrentLiabilitiesCurrent,
Sum(C.NetIncomeCurrent) AS NetIncomeCurrent,
--Sum(P.[Net Income Prior Year]) AS [Net Income Prior Year],
Sum(C.NetSalesCurrent) AS NetSalesCurrent,
--Sum(P.[Net Sales Prior Year]) AS [Net Sales Prior Year],
Sum(C.TotalAssetsCurrent) AS TotalAssetsCurrent
--Sum(P.[Total Assets Prior Year]) AS [Total Assets Prior Year]
into #AllData_IPE_C
from #AllData_Final_Stage_Current C
--left join #AllData_Final_Stage_Prior P
--ON C.ENTITYID = P.ENTITYID
 group by C.entityid


IF OBJECT_ID(N'tempdb..#AllData_IPE_P') IS NOT NULL
BEGIN
DROP TABLE #AllData_IPE_P;
END;
select 
P.entityid,
--Sum(CAST(C.CapitalExpenditures AS numeric)) AS CapitalExpenditures,
--Sum(C.CashMarketableSecurities) AS CashMarketableSecurities,
--Sum(C.CurrentMaturitiesLongTermDebt) AS CurrentMaturitiesLongTermDebt,
--DateOfFinancials,
--Sum(C.DepreciationAmortization) AS DepreciationAmortization,
--Sum(C.FixedAssets) AS FixedAssets,
--Sum(C.InterestExpense) AS InterestExpense,
--Sum(C.LongTermDebt) AS LongTermDebt,
--MinorityInterest,
--Sum(C.OperatingIncome) AS OperatingIncome,
--Sum(C.RetainedEarnings) AS RetainedEarnings,
--Sum(C.ShortTermDebt) AS ShortTermDebt,
--Sum(C.TangibleAssets) AS TangibleAssets,
--Sum(C.TotalLiabilities) AS TotalLiabilities,
--Sum(C.AccountsPayableCurrent) AS AccountsPayableCurrent,
Sum(P.[Accounts Payable (A/P) Prior Year]) AS [Accounts Payable (A/P) Prior Year],
--Sum(C.AccountsReceivableCurrent) AS AccountsReceivableCurrent,
Sum(P.[Accounts Receivable (A/R) Prior Year]) AS [Accounts Receivable (A/R) Prior Year],
Sum(P.[Inventory Prior Year]) AS [Inventory Prior Year],
--Sum(C.CurrentAssetsCurrent) AS CurrentAssetsCurrent,
Sum(P.[Current Assets Prior Year]) AS [Current Assets Prior Year],
--Sum(C.CurrentLiabilitiesCurrent) AS CurrentLiabilitiesCurrent,
--Sum(C.NetIncomeCurrent) AS NetIncomeCurrent,
Sum(P.[Net Income Prior Year]) AS [Net Income Prior Year],
--Sum(C.NetSalesCurrent) AS NetSalesCurrent,
Sum(P.[Net Sales Prior Year]) AS [Net Sales Prior Year],
--Sum(C.TotalAssetsCurrent) AS TotalAssetsCurrent,
Sum(P.[Total Assets Prior Year]) AS [Total Assets Prior Year],
Sum(P.CurrentLiabilitiesCurrent) AS CurrentLiabilitiesPriorYear
into #AllData_IPE_P
from #AllData_Final_Stage_Prior P
--left join #AllData_Final_Stage_Prior P
--ON C.ENTITYID = P.ENTITYID
 group by P.entityid


 
 
IF OBJECT_ID(N'tempdb..#AllData_IPE_CP') IS NOT NULL
BEGIN
DROP TABLE #AllData_IPE_CP;
END;
select 
C.entityid,
(CAST(C.CapitalExpenditures AS numeric)) AS CapitalExpenditures,
(C.CashMarketableSecurities) AS CashMarketableSecurities,
(C.CurrentMaturitiesLongTermDebt) AS CurrentMaturitiesLongTermDebt,
--DateOfFinancials,
(C.DepreciationAmortization) AS DepreciationAmortization,
(C.FixedAssets) AS FixedAssets,
(C.InterestExpense) AS InterestExpense,
(C.LongTermDebt) AS LongTermDebt,
(C.MinorityInterest) AS MinorityInterest,
(C.OperatingIncome) AS OperatingIncome,
(C.RetainedEarnings) AS RetainedEarnings,
(C.ShortTermDebt) AS ShortTermDebt,
(C.TangibleAssets) AS TangibleAssets,
(C.TotalLiabilities) AS TotalLiabilities,
(C.AccountsPayableCurrent) AS AccountsPayableCurrent,
(P.[Accounts Payable (A/P) Prior Year]) AS [Accounts Payable (A/P) Prior Year],
(C.AccountsReceivableCurrent) AS AccountsReceivableCurrent,
(P.[Accounts Receivable (A/R) Prior Year]) AS [Accounts Receivable (A/R) Prior Year],
(C.[Inventory Current]) AS [Inventory Current],
(P.[Inventory Prior Year]) AS [Inventory Prior Year],
(C.CurrentAssetsCurrent) AS CurrentAssetsCurrent,
(P.[Current Assets Prior Year]) AS [Current Assets Prior Year],
(C.CurrentLiabilitiesCurrent) AS CurrentLiabilitiesCurrent,
(P.CurrentLiabilitiesPriorYear) AS CurrentLiabilitiesPriorYear,
(C.NetIncomeCurrent) AS NetIncomeCurrent,
(P.[Net Income Prior Year]) AS [Net Income Prior Year],
(C.NetSalesCurrent) AS NetSalesCurrent,
(P.[Net Sales Prior Year]) AS [Net Sales Prior Year],
(C.TotalAssetsCurrent) AS TotalAssetsCurrent,
(P.[Total Assets Prior Year]) AS [Total Assets Prior Year]
into #AllData_IPE_CP
from #AllData_IPE_C C
left join #AllData_IPE_P P
ON C.ENTITYID = P.ENTITYID
 --group by C.entityid


 ----- Produce Final IPE for Client Fin's
 --drop table #TempH
 select
 A.ENTITYID,
 '' AS [Internal ID],
 '' AS [Original Internal ID],
 '' AS ObligorName, --C.CUSTOMERNAME AS [Obligor Name],
 CONVERT(DATE, D.DateOfFinancials) AS DateOfFinancials,
 '12/31/9999' AS DateLastAudit,
 A.NetSalesCurrent,
 A.[Net Sales Prior Year] AS NetSalesPriorYear,
 A.OperatingIncome,
 A.DepreciationAmortization,
 A.InterestExpense,
 A.NetIncomeCurrent,
 A.[Net Income Prior Year] AS NetIncomePriorYear,
 A.CashMarketableSecurities,
 A.AccountsReceivableCurrent,
 A.[Accounts Receivable (A/R) Prior Year] AS AccountsReceivablePriorYear,
  A.[Inventory Current],
 A.[Inventory Prior Year] AS InventoryPriorYear,
 A.CurrentAssetsCurrent,
 A.[Current Assets Prior Year] AS CurrentAssetsPriorYear,
 A.TangibleAssets,
 A.FixedAssets,
 A.TotalAssetsCurrent,
 A.[Total Assets Prior Year] AS TotalAssetsPriorYear,
 A.AccountsPayableCurrent,
 A.[Accounts Payable (A/P) Prior Year] AS AccountsPayablePriorYear,
 A.ShortTermDebt,
 A.CurrentMaturitiesLongTermDebt,
 A.CurrentLiabilitiesCurrent,
 A.CurrentLiabilitiesPriorYear,
 A.LongTermDebt,
 A.MinorityInterest,
 A.TotalLiabilities,
 A.RetainedEarnings,
 A.CapitalExpenditures
 --into #TempH
 from #AllData_IPE_CP A
 left join CreditLens.STG_SVB_Upcustomer C
 ON A.ENTITYID = C.CUSTOMERID
 left join (select max(Cast(STATEMENTDATEKEY_ as date)) AS DateOfFinancials,ENTITYID from #AllData_Final_Stage group by ENTITYID) D
 ON A.ENTITYID = D.ENTITYID
 --where A.ENTITYID = 51350

 --select * from #AllData_Final_Stage where ENTITYID = 51350