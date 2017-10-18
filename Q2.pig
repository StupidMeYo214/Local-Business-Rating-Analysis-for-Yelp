/*q2.pig
*/

ReviewOrigin = load '/mxs151730/hw1/review.csv' as line;
Review = FOREACH ReviewOrigin GENERATE FLATTEN((tuple(chararray,chararray,chararray,float))REGEX_EXTRACT_ALL(line,'(.*)\\:\\:(.*)\\:\\:(.*)\\:\\:(.*)')) AS (f1,f2,f3,f4);
ReviewTrim = foreach Review generate $2 as BID_Trim, $3 as Star;

BusinessOrigin = load '/mxs151730/hw1/business.csv' as line;
Business = foreach BusinessOrigin generate flatten((tuple(chararray,chararray,chararray))REGEX_EXTRACT_ALL(line,'(.*)\\:\\:(.*)\\:\\:(.*)')) AS (BID,ADDRESS,CAT);
BusinessFilteredTemp = filter Business by ADDRESS matches '.* CA.*';
BusinessFiltered = filter BusinessFilteredTemp by not ADDRESS matches '.*Palo Alto, CA.*';

AfterJoin = join BusinessFiltered by BID, ReviewTrim by BID_Trim;

AfterJoinFilterNull1 = foreach AfterJoin generate $0 as BID, $1 as ADDRESS, $2 as CAT, $4 as Star;

AfterGroup = group AfterJoinFilterNull1 by (BID, ADDRESS, CAT);

GroupAvg = foreach AfterGroup generate $0 as ID, AVG(AfterJoinFilterNull1.Star) as AVGRES;

AfterOrder = order GroupAvg by AVGRES desc;

res1 = limit AfterOrder 10;

store res1 into '/mxs151730/hw2/q2';