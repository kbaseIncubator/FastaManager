/*
A KBase module: FastaManager
*/

module FastaManager {
    typedef structure {
        string report_name;
        string report_ref;
    } BuildFastaOutput;

    funcdef build_fasta(UnspecifiedObject params) returns (BuildFastaOutput output)
        authentication required;


};
