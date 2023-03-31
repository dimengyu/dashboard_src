libname saf '\\nasn2ac.cc.emory.edu\surgery\SQOR\USRDS data\USRDS data (2021 SAF)\2021 Core\3_ESRD core 2021'; * USRDS SAF Files;
libname mdi "C:\Users\mdi\OneDrive - Emory University\Work\Jess-GenderDisparity\Data";
libname safcrwk "\\nasn2ac.cc.emory.edu\surgery\SQOR\USRDS data\USRDS data (2021 SAF)\Crosswalk\10_Provider and physician crosswalk 2021";
%include "C:\Users\mdi\OneDrive - Emory University\Work\Jess-GenderDisparity\Data\format.sas";
libname a "C:\Users\mdi\OneDrive - Emory University\Desktop\";
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
	*if 2012 <= year(referral_date) <= 2020;
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

	** referral outcomes;
	* overall referral;
	if referral_date > . then rf = 1; else rf = 0;
	* referral within 1 year;
	if mdy(1, 1, 2015) <= first_se <= mdy(6, 30, 2019) then do;
		rf_1yr_denom = 1;
		if . < referral_date - first_se + 1 <= 365.25 then rf_1yr = 1; 
		else rf_1yr = 0;
	end;

	** evaluation outcomes;
	* overall evaluation;
	if evaluation_start_date > . then eval = 1; else eval = 0;
	* evaluation within 3 months;
	if mdy(1, 1, 2015) <= referral_date <= mdy(3, 31, 2020) then do;
		eval_3m_denom = 1;
		if . < evaluation_start_date - referral_date + 1 <= 91.2501 then eval_3m = 1; 
		else eval_3m = 0;
	end;
	* evaluation within 6 months;
	if mdy(1, 1, 2015) <= referral_date <= mdy(12, 31, 2019) then do;
		eval_6m_denom = 1;
		if . < evaluation_start_date - referral_date + 1 <= 182.5 then eval_6m = 1; 
		else eval_6m = 0;
	end;

	** waitlisting outcomes;
	* overall waitlisting;
	if edate > . then wl = 1; else wl = 0;
	* waitllisting within 6 months from evaluation;
	if mdy(1, 1, 2015) <= evaluation_start_date <= mdy(6, 30, 2020) then do;
		wl_6m_denom = 1;
		if . < edate - evaluation_start_date + 1 <= 182.5 then wl_6m = 1; 
		else wl_6m = 0;
	end;
	* waitllisting within 2 years from dialysis start;
	if mdy(1, 1, 2015) <= first_se <= mdy(12, 31, 2018) then do;
		wl_2yr_denom = 1;
		if . < edate - first_se + 1 <= 730.5 then wl_2yr = 1; 
		else wl_2yr = 0;
	end;
run;

proc sql;
	create table a.tx_outcome as 
		select *, b.rf_1yr_cnt*100/a.rf_1yr_denom as rf_1yr_pct 
		from (select nw_denom as nw, txctr_denom as tx_ctr, 
					 sum(rf_1yr_denom) as rf_1yr_denom
			  from a.outcome_pt
			  group by nw_denom, txctr_denom) a 
		left join 
			 (select nw, tx_ctr,
			 		 max(sum(rf_1yr), 0) as rf_1yr_cnt,
					 max(sum(eval_3m)*100/sum(eval_3m_denom), 0) as eval_3m_pct,
					 max(sum(eval_6m)*100/sum(eval_6m_denom), 0) as eval_6m_pct,
					 max(sum(wl_6m)*100/sum(wl_6m_denom), 0) as wl_6m_pct,
					 max(sum(wl_2yr)*100/sum(wl_2yr_denom), 0) as wl_2yr_pct
			  from a.outcome_pt
			  group by nw, tx_ctr) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr;

	create table a.tx_outcome_nopreemprf as 
		select *, b.rf_1yr_cnt*100/a.rf_1yr_denom as rf_1yr_pct 
		from (select nw_denom as nw, txctr_denom as tx_ctr, 
					 sum(rf_1yr_denom) as rf_1yr_denom
			  from (select * from a.outcome_pt where preempt_rf ~= 1)
			  group by nw_denom, txctr_denom) a 
		left join 
			 (select nw, tx_ctr,
			 		 max(sum(rf_1yr), 0) as rf_1yr_cnt,
					 max(sum(eval_3m)*100/sum(eval_3m_denom), 0) as eval_3m_pct,
					 max(sum(eval_6m)*100/sum(eval_6m_denom), 0) as eval_6m_pct,
					 max(sum(wl_6m)*100/sum(wl_6m_denom), 0) as wl_6m_pct,
					 max(sum(wl_2yr)*100/sum(wl_2yr_denom), 0) as wl_2yr_pct
			  from (select * from a.outcome_pt where preempt_rf ~= 1)
			  group by nw, tx_ctr) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr;
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
data date_denom_nopre;
	set date_denom;
	if preempt_rf ~= 1;
run;

proc sql;
	create table rf_pct_month as
		select a.nw, a.tx_ctr, a.date, max(0, b.rf_1yr)*100/a.denom_cnt as rf_1yr_pct
		from (select nw_denom as nw, 
					 txctr_denom as tx_ctr, 
					 rf_date_denom as date, 
					 count(*) as denom_cnt
			  from date_denom
			  group by nw_denom, txctr_denom, rf_date_denom) a 
		left join (select nw, tx_ctr, 
						  rf_date_denom as date, 
						  sum(rf_1yr) as rf_1yr
				   from date_denom 
				   group by nw, tx_ctr, rf_date_denom) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr and a.date = b.date
		where mdy(1, 1, 2015) <= a.date <= mdy(6, 30, 2020) and a.nw > .;

	create table eval_pct_month as
		select a.nw, a.tx_ctr, a.date, max(0, b.eval_3m)*100/a.denom_cnt as eval_3m_pct
		from (select nw, tx_ctr, 
					 eval_date_denom as date, 
					 count(*) as denom_cnt
			  from date_denom
			  group by nw, tx_ctr, eval_date_denom) a 
		left join (select nw, tx_ctr, 
						  eval_date_denom as date, 
						  sum(eval_3m) as eval_3m
				   from date_denom 
				   group by nw, tx_ctr, eval_date_denom) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr and a.date = b.date
		where mdy(1, 1, 2015) <= a.date <= mdy(6, 30, 2020) and a.nw > .;

	create table eval2_pct_month as
		select a.nw, a.tx_ctr, a.date, max(0, b.eval_6m)*100/a.denom_cnt as eval_6m_pct
		from (select nw, tx_ctr, 
					 eval_date_denom as date, 
					 count(*) as denom_cnt
			  from date_denom
			  group by nw, tx_ctr, eval_date_denom) a 
		left join (select nw, tx_ctr, 
						  eval_date_denom as date, 
						  sum(eval_6m) as eval_6m
				   from date_denom 
				   group by nw, tx_ctr, eval_date_denom) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr and a.date = b.date
		where mdy(1, 1, 2015) <= a.date <= mdy(6, 30, 2020) and a.nw > .;

	create table wl_pct_month as
		select a.nw, a.tx_ctr, a.date, max(0, b.wl_6m)*100/a.denom_cnt as wl_6m_pct
		from (select nw, tx_ctr, 
					 wl_date_denom as date, 
					 count(*) as denom_cnt
			  from date_denom
			  group by nw, tx_ctr, wl_date_denom) a 
		left join (select nw, tx_ctr, 
						  wl_date_denom as date, 
						  sum(wl_6m) as wl_6m
				   from date_denom 
				   group by nw, tx_ctr, wl_date_denom) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr and a.date = b.date
		where mdy(1, 1, 2015) <= a.date <= mdy(6, 30, 2020) and a.nw > .;

	create table wl2_pct_month as
		select a.nw, a.tx_ctr, a.date, max(0, b.wl_2yr)*100/a.denom_cnt as wl_2yr_pct
		from (select nw_denom as nw, 
					 txctr_denom as tx_ctr, 
					 wl_among_all_date_denom as date, 
					 count(*) as denom_cnt
			  from date_denom
			  group by nw_denom, txctr_denom, wl_among_all_date_denom) a 
		left join (select nw, tx_ctr, 
						  wl_among_all_date_denom as date, 
						  sum(wl_2yr) as wl_2yr
				   from date_denom 
				   group by nw, tx_ctr, wl_among_all_date_denom) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr and a.date = b.date
		where mdy(1, 1, 2015) <= a.date <= mdy(6, 30, 2020) and a.nw > .;

	*********************************************************************************************************
	*********************************************************************************************************;

	create table rf_pct_month_nopre as
		select a.nw, a.tx_ctr, a.date, max(0, b.rf_1yr)*100/a.denom_cnt as rf_1yr_pct_nopre
		from (select nw_denom as nw, 
					 txctr_denom as tx_ctr, 
					 rf_date_denom as date, 
					 count(*) as denom_cnt
			  from date_denom_nopre
			  group by nw_denom, txctr_denom, rf_date_denom) a 
		left join (select nw, tx_ctr, 
						  rf_date_denom as date, 
						  sum(rf_1yr) as rf_1yr
				   from date_denom_nopre 
				   group by nw, tx_ctr, rf_date_denom) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr and a.date = b.date
		where mdy(1, 1, 2015) <= a.date <= mdy(6, 30, 2020) and a.nw > .;

	create table eval_pct_month_nopre as
		select a.nw, a.tx_ctr, a.date, max(0, b.eval_3m)*100/a.denom_cnt as eval_3m_pct_nopre
		from (select nw, tx_ctr, 
					 eval_date_denom as date, 
					 count(*) as denom_cnt
			  from date_denom_nopre
			  group by nw, tx_ctr, eval_date_denom) a 
		left join (select nw, tx_ctr, 
						  eval_date_denom as date, 
						  sum(eval_3m) as eval_3m
				   from date_denom_nopre 
				   group by nw, tx_ctr, eval_date_denom) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr and a.date = b.date
		where mdy(1, 1, 2015) <= a.date <= mdy(6, 30, 2020) and a.nw > .;

	create table eval2_pct_month_nopre as
		select a.nw, a.tx_ctr, a.date, max(0, b.eval_6m)*100/a.denom_cnt as eval_6m_pct_nopre
		from (select nw, tx_ctr, 
					 eval_date_denom as date, 
					 count(*) as denom_cnt
			  from date_denom_nopre
			  group by nw, tx_ctr, eval_date_denom) a 
		left join (select nw, tx_ctr, 
						  eval_date_denom as date, 
						  sum(eval_6m) as eval_6m
				   from date_denom_nopre 
				   group by nw, tx_ctr, eval_date_denom) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr and a.date = b.date
		where mdy(1, 1, 2015) <= a.date <= mdy(6, 30, 2020) and a.nw > .;

	create table wl_pct_month_nopre as
		select a.nw, a.tx_ctr, a.date, max(0, b.wl_6m)*100/a.denom_cnt as wl_6m_pct_nopre
		from (select nw, tx_ctr, 
					 wl_date_denom as date, 
					 count(*) as denom_cnt
			  from date_denom_nopre
			  group by nw, tx_ctr, wl_date_denom) a 
		left join (select nw, tx_ctr, 
						  wl_date_denom as date, 
						  sum(wl_6m) as wl_6m
				   from date_denom_nopre 
				   group by nw, tx_ctr, wl_date_denom) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr and a.date = b.date
		where mdy(1, 1, 2015) <= a.date <= mdy(6, 30, 2020) and a.nw > .;

	create table wl2_pct_month_nopre as
		select a.nw, a.tx_ctr, a.date, max(0, b.wl_2yr)*100/a.denom_cnt as wl_2yr_pct_nopre
		from (select nw_denom as nw, 
					 txctr_denom as tx_ctr, 
					 wl_among_all_date_denom as date, 
					 count(*) as denom_cnt
			  from date_denom_nopre
			  group by nw_denom, txctr_denom, wl_among_all_date_denom) a 
		left join (select nw, tx_ctr, 
						  wl_among_all_date_denom as date, 
						  sum(wl_2yr) as wl_2yr
				   from date_denom_nopre 
				   group by nw, tx_ctr, wl_among_all_date_denom) b
		on a.nw = b.nw and a.tx_ctr = b.tx_ctr and a.date = b.date
		where mdy(1, 1, 2015) <= a.date <= mdy(6, 30, 2020) and a.nw > .;
quit;

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
	if date <= mdy(6, 30, 2020);
run;

data outcome_month;
	merge dt rf_pct_month eval_pct_month eval2_pct_month wl_pct_month wl2_pct_month 
		  rf_pct_month_nopre eval_pct_month_nopre eval2_pct_month_nopre wl_pct_month_nopre wl2_pct_month_nopre;
	by nw tx_ctr date;
run;

proc stdize data = outcome_month out=a.outcome_month reponly missing=0;
run;
