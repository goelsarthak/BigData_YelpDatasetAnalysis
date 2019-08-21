reviews = LOAD 'hdfs://localhost:9000/FinalProject/Dataset/yelp_review.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');

describe reviews;

users = LOAD 'hdfs://localhost:9000/FinalProject/Dataset/yelp_user.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');

describe users;


fltrvw = FOREACH reviews GENERATE (chararray) $0 as reviewId, (chararray) $1 as userId, (chararray) $2 as businessID, (chararray) $3 as rating, (chararray) $4 as date, (chararray) $5 as userReview;


describe fltrvw;


fltusr = FOREACH users GENERATE (chararray) $0 as userId, (chararray) $1 as userName;

describe fltusr;

joined = Join fltrvw by userId, fltusr by userId USING 'replicated';

lmtjnd = LIMIT joined 5;

Dump lmtjnd;

STORE lmtjnd INTO '/Users/sarthakgoel/Desktop/pig' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');

