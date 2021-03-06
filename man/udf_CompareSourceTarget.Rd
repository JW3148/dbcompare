% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/tblcompare.R
\name{udf_CompareSourceTarget}
\alias{udf_CompareSourceTarget}
\title{Compare a list of tables on source server and target server}
\usage{
udf_CompareSourceTarget(source_server, target_server, db, proc)
}
\arguments{
\item{source_server}{Name of source SQL Server Instance}

\item{target_server}{Name of target SQL Server Instance}

\item{db}{Name of database}

\item{proc}{Name of stored procedure}
}
\value{
a list contains updated tables, new tables, missing tables and identical tables
}
\description{
Takes in source server, target server, database and procedure, and return information on updated tables, new tables,
    missing tables and identical tables
}
