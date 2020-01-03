# nifi-marklogic-sas

*SAS7BDAT in, JSON out*

A NiFi Processor for converting SAS7BDAT to JSON arrays. It uses [mlsastools](https://github.com/mikrovvelle/mlsastools) and thereby [parso](https://github.com/epam/parso) to get the tabular data out of incoming files.

The resulting JSON is a an array of objects, where each object represents one row of the SAS data. All objects have the same keys, where each key represents one column from the SAS data. The value for each field represents the value for the corresponding row and column.

If you're ingesting this data into MarkLogic or another document-oriented database, you probably want to follow the document-as-row convention, so you would then use the [SplitJson component](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.9.2/org.apache.nifi.processors.standard.SplitJson/index.html) subsequent to this one, and then ingest each "row" document as an individual JSON object.


