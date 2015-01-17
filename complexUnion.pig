setA =
    LOAD 'mySource-1.csv'
    USING PigStorage(';')
    AS (
        field1: chararray,
        field2: chararray
    );

setB =
    LOAD 'mySource-2.csv'
    USING PigStorage(';')
    AS (
        field1: chararray,
        field2: chararray
    );

result = Union setA, setB;

STORE result INTO 'output.csv' USING PigStorage(';');