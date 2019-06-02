LIBNAME GBQ ODBC DSN="Google BigQuery";
LIBNAME WQD7005 "C:\Users\vmann\Documents\My SAS Files\SAS Lib\wqd7005";

data WQD7005.StockPrice;
	set GBQ.StockPrice_;
run;

data WQD7005.BusinessNews;
	set GBQ.BusinessNews_;
run;

data WQD7005.Commodities;
	set GBQ.Commodities_;
	if Symbol='GC=F' then Symbol='GC';
	if Symbol='SI=F' then Symbol='SI';
	if Symbol='CL=F' then Symbol='CL';
run;

PROC SORT data=WQD7005.Commodities;
    BY Date;
RUN;

proc transpose data=WQD7005.Commodities out=WQD7005.Commodities(drop=_name_);
   var LastPrice;
   id Symbol;
   by Date;
   where Symbol in ('GC','SI','CL');
run;

data WQD7005.ForumPosts;
	set GBQ.ForumPosts_;
run;

data WQD7005.MajorIndices;
	set GBQ.MajorIndices_;
	if Symbol='^IXIC' then Symbol='IXIC';
	else if Symbol='^KLSE' then Symbol='KLSE';
	else if Symbol='^N100' then Symbol='N100';
	else if Symbol='000001.SS' then Symbol='SS';
	else if Symbol='^HSI' then Symbol='HSI';
	else if Symbol='^FTSE' then Symbol='FTSE';
	else if Symbol='^BSESN' then Symbol='BSESN';
	else if Symbol='^N225' then Symbol='N225';
run;

PROC SORT data=WQD7005.MajorIndices;
    BY Date;
RUN;

proc transpose data=WQD7005.MajorIndices out=WQD7005.MajorIndices(drop=_name_);
   var LastPrice;
   id Symbol;
   by Date;
   where Symbol in ('IXIC','KLSE','N100','SS', 'HSI', 'FTSE', 'BSESN', 'N225');
run;

data WQD7005.StocksTweetFeed;
	set GBQ.StocksTweetFeed_;
run;

data WQD7005.MajorForex;
	set GBQ.MajorForex_;
run;

PROC SORT data=WQD7005.MajorForex;
    BY UpdateDate;
RUN;
 
proc transpose data=WQD7005.MajorForex out=WQD7005.MajorForex(drop=_name_);
   var UnitsPerMYR;
   id CurrencyCode;
   by UpdateDate;
   where CurrencyCode in ('USD','EUR');
run;

data WQD7005.MajorForex;
set WQD7005.MajorForex (rename = (UpdateDate = Date));

run;