/*q3
*/
ReviewOrigin = load '/mxs151730/hw1/review.csv' as line;
Review = FOREACH ReviewOrigin GENERATE FLATTEN((tuple(chararray,chararray,chararray,float))REGEX_EXTRACT_ALL(line,'(.*)\\:\\:(.*)\\:\\:(.*)\\:\\:(.*)')) AS (f1,f2,f3,f4);
BusinessOrigin = load '/mxs151730/hw1/business.csv' as line;
Business = foreach BusinessOrigin generate flatten((tuple(chararray,chararray,chararray))REGEX_EXTRACT_ALL(line,'(.*)\\:\\:(.*)\\:\\:(.*)')) AS (BID,ADDRESS,CAT);

ResTemp = cogroup Review by f3, Business by BID;
Res3 = limit ResTemp 5;

store Res3 into '/mxs151730/hw2/q3';