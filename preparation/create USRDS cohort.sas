libname saf '\\nasn2ac.cc.emory.edu\surgery\SQOR\USRDS data\USRDS data (2021 SAF)\2021 Core\3_ESRD core 2021'; * USRDS SAF Files;
libname mdi "C:\Users\mdi\OneDrive - Emory University\Work\Jess-GenderDisparity\Data";
libname safcrwk "\\nasn2ac.cc.emory.edu\surgery\SQOR\USRDS data\USRDS data (2021 SAF)\Crosswalk\10_Provider and physician crosswalk 2021";
%include "C:\Users\mdi\OneDrive - Emory University\Work\Jess-GenderDisparity\Data\format.sas";
libname a "C:\Users\mdi\OneDrive - Emory University\Desktop\Tx dashboard";
options nofmterr;

*****************************************************************
************************** Base Dataset *************************
*****************************************************************;
data rxhist; 
	set saf.rxhist60;
	if begday = 1;
	if rxgroup in ("1", "2", "3", "5", "7", "9", "T");
	keep usrds_id;
run;

************* prepare WL dataset *************;
proc sort data = saf.waitlist_ki out = wl;
	by usrds_id edate;
run;

data wl;
	set wl;
	by usrds_id edate;
	if first.usrds_id;
	if usrds_id > .;
run;


proc sort data = saf.medevid out = medevid;
	by USRDS_ID descending CRDATE; 
run;

data medevid;
	set medevid;
	by USRDS_ID descending CRDATE; 
	if first.USRDS_ID;

	if medicalcoverage = '1' then insurance_esrd = 1; *Medicaid;
		else if medicalcoverage = '2' then insurance_esrd=4;  *DVA = Other coverage;
		else if medicalcoverage='3' then insurance_esrd=2;  *Medicare;
		else if medicalcoverage='4' then insurance_esrd = 2;	*Med Advantage;
		else if medicalcoverage='5' then insurance_esrd=3;  *Employer ;
		else if medicalcoverage='6' then insurance_esrd=4; *Other = other;
		else if medicalcoverage='7' then insurance_esrd=5; *No insurance;
		else if medicalcoverage='1,2' then insurance_esrd=1; *Medicaid and DVA is Medicaid;
		else if medicalcoverage='1,3' then insurance_esrd=1; *Medicaid and Medicare is Medicaid;
		else if medicalcoverage='1,3,4' then insurance_esrd=1; *Medicaid, Medicare, Med Advantage = medicaid;
		else if medicalcoverage='1,3,5' then insurance_esrd=3; *Medicaid, Medicare, and Employer counts as employer;
		else if medicalcoverage='1,3,5,6' then insurance_esrd=3; *Medicaid, Medicare, Employer, and Other = employer;
		else if medicalcoverage='1,3,6' then insurance_esrd=1; *Medicaid, Medicare, and Other = Medicaid;
		else if medicalcoverage='1,4' then insurance_esrd=1; *Medicaid and Medicare Advantage = Medicaid;
		else if medicalcoverage='1,4,5' then insurance_esrd=3; *Medicaid, Medicare Advantage, and Employer = employer;
		else if medicalcoverage='1,5' then insurance_esrd=3; *Medicaid and Employer = employer;
		else if medicalcoverage='1,5,6' then insurance_esrd=3; *Medicaid, Employer, and Other = employer;
		else if medicalcoverage='1,6' then insurance_esrd=1; *Medicaid and Other = medicaid;
		else if medicalcoverage='2,5' then insurance_esrd=3; *DVA and Employer = employer;
		else if medicalcoverage='2,6' then insurance_esrd=4; *DVA and Other = other;
		else if medicalcoverage='3,4' then insurance_esrd=2; *Medicare and Medicare Advantage = medicare;
		else if medicalcoverage='3,5' then insurance_esrd=3; *Medicare and Employer = employer;
		else if medicalcoverage='3,5,6' then insurance_esrd=3; *Medicare, Employer, and Other' = employer;
		else if medicalcoverage='3,6' then insurance_esrd=2; *Medicare and Other = medicare;
		else if medicalcoverage='5,6' then insurance_esrd=3; *Employer and Other = employer;

	***For medical evidence 1995;
	if MEDCOV_GROUP='Y' then insurance_esrd=3; *Employer ;
	if MEDCOV_MDCD='Y' and MEDCOV_GROUP='N' then insurance_esrd=1; *Medicaid = public;
	if MEDCOV_MDCR='Y' and MEDCOV_GROUP = 'N' then insurance_esrd=2; *Medicare = public;
	if MEDCOV_DVA='Y' and MEDCOV_GROUP = 'N' then insurance_esrd=4; *DVA = other insurance;
	if MEDCOV_OTHER='Y' and MEDCOV_GROUP = 'N' and MEDCOV_MDCD='N' and MEDCOV_MDCR='N' and MEDCOV_DVA='N' then insurance_esrd=4; *Other;
	if MEDCOV_NONE='Y' and MEDCOV_OTHER='N' and MEDCOV_GROUP='N' and MEDCOV_MDCD='N' and MEDCOV_MDCR='N' then insurance_esrd=5; *No insurance;
		else if MEDCOV_MDCD='Y' and MEDCOV_MDCR='Y' then insurance_esrd=1;	*Medicaid;
		else if MEDCOV_MDCD='Y' and MEDCOV_OTHER='Y' then insurance_esrd=1; *Medicaid;
		else if MEDCOV_MDCD='Y' and MEDCOV_GROUP='Y' then insurance_esrd=3; *Employer;
		else if MEDCOV_MDCR='Y' and MEDCOV_OTHER='Y' then insurance_esrd=2; *Medicare;
		else if MEDCOV_MDCR='Y' and MEDCOV_GROUP='Y' then insurance_esrd=3; *Employer;

	format insurance_esrd insure.; label insurance_esrd = "Insurance Status";
	keep usrds_id insurance_esrd;
run;

*********************** Prepare referral data ***********************;
data rf;
	set mdi.rf_allnws;
	if 2012 <= year(referral_date) <= 2020;
	if USRDS_ID > .;
run;

proc sort data = rf nodupkey;
	by USRDS_ID referral_date;
run;

data rf;
	set rf(rename = (masked_data_access_group = tx_ctr));
	by USRDS_ID referral_date;
	if first.usrds_id;	
	keep nw tx_ctr USRDS_ID referral_date evaluation_start_date corrected_ccn;
run;

proc sql;
	create table cohort as 
		select a.*, b.edate, c.insurance_esrd, d.* 
		from (select * from saf.patients where year(first_se) >= 2015 and inc_age >= 18 and ~missing(ZIPCODE) and USRDS_ID in (select USRDS_ID from rxhist)) a 
		left join wl b 
		on a.USRDS_ID = b.USRDS_ID
		left join medevid c
		on a.USRDS_ID = c.USRDS_ID
		left join rf d
		on a.USRDS_ID = d.USRDS_ID;

	drop table wl, ID_PROVUSRD, ID_ZIP, fc, medevid, rf, rxhist;
quit;

*****************************************************************
************************** Denominator **************************
*****************************************************************;
PROC IMPORT OUT= TX_TRR 
            DATAFILE= "C:\Users\mdi\OneDrive - Emory University\Work\Jess-GenderDisparity\Data\Tx CTR list.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="TX_TRR$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

PROC IMPORT OUT= TRR_ZIPCODE
            DATAFILE= "C:\Users\mdi\OneDrive - Emory University\Work\Jess-GenderDisparity\Data\Tx CTR list.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="TRR_ZIPCODE$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

proc sql;
	create table TX_DENOM as
		select a.Network as nw_denom, a.Tx_center_ID_in_Datafile_B as txctr_denom, c.*   
		from tx_trr a left join trr_zipcode b
		on a.TRR = b.TRR 
		left join cohort c
		on b.ZIP = c.ZIPCODE
		where c.USRDS_ID > .;

	drop table trr_zipcode;
quit;

data a.outcome_pt;
	set TX_DENOM;
	
	* exclude preemptively waitlisted patietns;
	if . < edate < first_se then delete;

	* define preemptively referred patietns;
	if . < referral_date < first_se then preempt_rf = 1; else preempt_rf = 0;

	**** preemptively referred patients have all outcomes; 
	if preempt_rf = 1 then do;
		rf = 1; rf_1yr = 1; eval = 1; eval_3m = 1; eval_6m = 1; wl = 1; wl_6m = 1; wl_2yr = 1; 
	end;

	else do;
		** referral outcomes;
		* overall referral;
		if referral_date > . then rf = 1; else rf = 0;
		* referral within 1 year;
		if mdy(1, 1, 2015) <= first_se <= mdy(6, 30, 2019) then do;
			if . < referral_date - first_se + 1 <= 365.25 then rf_1yr = 1; 
			else rf_1yr = 0;
		end;

		** evaluation outcomes;
		* overall evaluation;
		if evaluation_start_date > . then eval = 1; else eval = 0;
		* evaluation within 3 months;
		if mdy(1, 1, 2015) <= referral_date <= mdy(3, 31, 2020) then do;
			if . < evaluation_start_date - referral_date + 1 <= 91.2501 then eval_3m = 1; 
			else eval_3m = 0;
		end;
		* evaluation within 6 months;
		if mdy(1, 1, 2015) <= referral_date <= mdy(12, 31, 2019) then do;
			if . < evaluation_start_date - referral_date + 1 <= 182.5 then eval_6m = 1; 
			else eval_6m = 0;
		end;

		** waitlisting outcomes;
		* overall waitlisting;
		if edate > . then wl = 1; else wl = 0;
		* waitllisting within 6 months from evaluation;
		if mdy(1, 1, 2015) <= evaluation_start_date <= mdy(6, 30, 2020) then do;
			if . < edate - evaluation_start_date + 1 <= 182.5 then wl_6m = 1; 
			else wl_6m = 0;
		end;
		* waitllisting within 2 years from dialysis start;
		if mdy(1, 1, 2015) <= first_se <= mdy(12, 31, 2018) then do;
			if . < edate - first_se + 1 <= 730.5 then wl_2yr = 1; 
			else wl_2yr = 0;
		end;
	end;
run;

proc sql;
	create table tx_outcome_denom as 
		select nw_denom as nw, txctr_denom as tx_ctr, count(*) as denom
		from a.outcome_pt
		group by nw_denom, txctr_denom;

	create table a.tx_outcome as 
		select a.nw, a.tx_ctr, 
			   a.rf_1yr_cnt*100/b.denom as rf_1yr_pct, 
			   a.eval_3m_pct, a.eval_6m_pct, a.wl_6m_pct,
			   a.wl_2yr_cnt*100/b.denom as wl_2yr_pct
		from (select nw, tx_ctr, 
					 sum(rf_1yr) as rf_1yr_cnt, 
					 sum(eval_3m)*100/count(eval_3m) as eval_3m_pct, 
					 sum(eval_6m)*100/count(eval_6m) as eval_6m_pct, 
					 sum(wl_6m)*100/count(wl_6m) as wl_6m_pct, 
					 sum(wl_2yr) as wl_2yr_cnt 
					 from a.outcome_pt 
					 group by nw, tx_ctr) a 
		left join tx_outcome_denom b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr
		where ~missing(a.nw);

	create table tx_outcome_denom as 
		select nw_denom as nw, txctr_denom as tx_ctr, count(*) as denom
		from (select * from a.outcome_pt where preempt_rf ~= 1)
		group by nw_denom, txctr_denom;
	
	create table a.tx_outcome_nopreemprf as 
		select a.nw, a.tx_ctr, 
			   a.rf_1yr_cnt*100/b.denom as rf_1yr_pct, 
			   a.eval_3m_pct, a.eval_6m_pct, a.wl_6m_pct,
			   a.wl_2yr_cnt*100/b.denom as wl_2yr_pct
		from (select nw, tx_ctr, 
					 sum(rf_1yr) as rf_1yr_cnt, 
					 sum(eval_3m)*100/count(eval_3m) as eval_3m_pct, 
					 sum(eval_6m)*100/count(eval_6m) as eval_6m_pct, 
					 sum(wl_6m)*100/count(wl_6m) as wl_6m_pct, 
					 sum(wl_2yr) as wl_2yr_cnt 
					 from (select * from a.outcome_pt where preempt_rf ~= 1)
					 group by nw, tx_ctr) a 
		left join tx_outcome_denom b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr
		where ~missing(a.nw);

	drop table tx_outcome_denom;
quit;

****************************************** Percentage per month *********************************************;
data date_denom;
	set a.outcome_pt;
	rf_date_denom = intnx('month', min(first_se, referral_date), 0);
	eval_date_denom = intnx('month', min(referral_date, evaluation_start_date), 0);
	wl_date_denom = intnx('month', min(evaluation_start_date, edate), 0);
	wl_among_all_date_denom = intnx('month', min(first_se, edate), 0);
	format rf_date_denom eval_date_denom wl_date_denom wl_among_all_date_denom mmddyy10.;
run;

data dt;
	set tx_trr;
	do yr = 2015 to 2020;
		do m = 1 to 12;
			date = mdy(m, 1, yr);
			output;
		end;
	end;
	format date mmddyy10.;
	keep network Tx_center_ID_in_Datafile_B date;
	rename network = nw Tx_center_ID_in_Datafile_B = tx_ctr;
run;


proc sql;
	create table test as
		select nw_denom as nw, txctr_denom as tx_ctr, 
			   rf_date_denom as date, count(*) as denom_cnt
			   from date_denom 
			   group by nw_denom, txctr_denom, rf_date_denom;

	create table rf_pct_month as
		select a.nw, a.tx_ctr, a.date, max(0, b.rf_1yr)*100/a.denom_cnt as rf_1yr_pct
		from test a left join (select nw, tx_ctr, rf_date_denom as date, sum(rf_1yr) as rf_1yr
							   from date_denom 
							   group by nw, tx_ctr, rf_date_denom
							   having ~missing(nw)) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr and a.date = b.date;

	create table eval_pct_month as
		select nw, tx_ctr, eval_date_denom as date, 
			   sum(eval_3m)*100/count(eval_3m) as eval_3m_pct, 
			   sum(eval_6m)*100/count(eval_6m) as eval_6m_pct
		from date_denom
		group by nw, tx_ctr, eval_date_denom
		having ~missing(nw);

	create table wl_pct_month as
		select nw, tx_ctr, wl_date_denom as date, 
			   sum(wl_6m)*100/count(wl_6m) as wl_6m_pct
		from date_denom
		group by nw, tx_ctr, wl_date_denom
		having ~missing(nw);

	create table wl_among_all_pct_month as
		select a.nw, a.tx_ctr, a.date, max(0, b.wl_2yr)*100/a.denom_cnt as wl_2yr_pct
		from test a left join (select nw, tx_ctr, wl_among_all_date_denom as date, sum(wl_2yr) as wl_2yr
							   from date_denom 
							   group by nw, tx_ctr, wl_among_all_date_denom
							   having ~missing(nw)) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr and a.date = b.date;

	*********************************************************************************************************
	*********************************************************************************************************;
	create table test as
		select nw_denom as nw, txctr_denom as tx_ctr, 
			   rf_date_denom as date, count(*) as denom_cnt
			   from (select * from date_denom where preempt_rf ~= 1)
			   group by nw_denom, txctr_denom, rf_date_denom;

	create table rf_pct_month_nopre as
		select a.nw, a.tx_ctr, a.date, max(0, b.rf_1yr)*100/a.denom_cnt as rf_1yr_pct_nopre
		from test a left join (select nw, tx_ctr, rf_date_denom as date, sum(rf_1yr) as rf_1yr
							   from (select * from date_denom where preempt_rf ~= 1) 
							   group by nw, tx_ctr, rf_date_denom
							   having ~missing(nw)) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr and a.date = b.date;


	create table eval_pct_month_nopre as
		select nw, tx_ctr, eval_date_denom as date, 
			   sum(eval_3m)*100/count(eval_3m) as eval_3m_pct_nopre, 
			   sum(eval_6m)*100/count(eval_6m) as eval_6m_pct_nopre
		from (select * from date_denom where preempt_rf ~= 1)
		group by nw, tx_ctr, eval_date_denom
		having ~missing(nw);

	create table wl_pct_month_nopre as
		select nw, tx_ctr, wl_date_denom as date, 
			   sum(wl_6m)*100/count(wl_6m) as wl_6m_pct_nopre
		from (select * from date_denom where preempt_rf ~= 1)
		group by nw, tx_ctr, wl_date_denom
		having ~missing(nw);

	create table wl_among_all_pct_month_nopre as
		select a.nw, a.tx_ctr, a.date, max(0, b.wl_2yr)*100/a.denom_cnt as wl_2yr_pct_nopre
		from test a left join (select nw, tx_ctr, wl_among_all_date_denom as date, sum(wl_2yr) as wl_2yr
							   from (select * from date_denom where preempt_rf ~= 1)
							   group by nw, tx_ctr, wl_among_all_date_denom
							   having ~missing(nw)) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr and a.date = b.date;


	create table outcome_month as
		select z.*, a.rf_1yr_pct, b.eval_3m_pct, b.eval_6m_pct, c.wl_6m_pct, g.wl_2yr_pct,
			   d.rf_1yr_pct_nopre, e.eval_3m_pct_nopre, e.eval_6m_pct_nopre, f.wl_6m_pct_nopre, h.wl_2yr_pct_nopre
		from dt z left join rf_pct_month a
		on z.nw = a.nw and z.tx_ctr = a.tx_ctr and z.date = a.date
		left join eval_pct_month b
		on z.nw = b.nw and z.tx_ctr = b.tx_ctr and z.date = b.date
		left join wl_pct_month c
		on z.nw = c.nw and z.tx_ctr = c.tx_ctr and z.date = c.date
		left join rf_pct_month_nopre d
		on z.nw = d.nw and z.tx_ctr = d.tx_ctr and z.date = d.date
		left join eval_pct_month_nopre e
		on z.nw = e.nw and z.tx_ctr = e.tx_ctr and z.date = e.date
		left join wl_pct_month_nopre f
		on z.nw = f.nw and z.tx_ctr = f.tx_ctr and z.date = f.date
		left join wl_among_all_pct_month g
		on z.nw = g.nw and z.tx_ctr = g.tx_ctr and z.date = g.date
		left join wl_among_all_pct_month_nopre h
		on z.nw = h.nw and z.tx_ctr = h.tx_ctr and z.date = h.date
		where z.date <= mdy(6, 30, 2020);
quit;

proc stdize data = outcome_month out=a.outcome_month reponly missing=0;
run;
