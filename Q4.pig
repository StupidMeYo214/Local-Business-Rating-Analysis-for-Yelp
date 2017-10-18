/*q4
*/

ReviewOrigin = load '/mxs151730/hw1/review.csv' as line;
Review = FOREACH ReviewOrigin GENERATE FLATTEN((tuple(chararray,chararray,chararray,float))REGEX_EXTRACT_ALL(line,'(.*)\\:\\:(.*)\\:\\:(.*)\\:\\:(.*)')) AS (RID,UID,RBID,STAR);
BusinessOrigin = load '/mxs151730/hw1/business.csv' as line;
Business = foreach BusinessOrigin generate flatten((tuple(chararray,chararray,chararray))REGEX_EXTRACT_ALL(line,'(.*)\\:\\:(.*)\\:\\:(.*)')) AS (BID,ADDRESS,CAT);

BusinessFiltered = filter Business by ADDRESS matches '.*Stanford.*';
IDMatches = foreach BusinessFiltered generate $0 as BID;

AfterJoin = join IDMatches by BID, Review by RBID;
--BID,RID,UID,RBID,STAR

RESTEMP = foreach AfterJoin generate $2 as UID, $4 as RATE;

RES4 = limit RESTEMP 10;

store RES4 into '/mxs151730/hw2/q4';