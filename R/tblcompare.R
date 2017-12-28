#' Get all columns for a given table
#'
#' Takes in name of a table, database and the instance of a SQL Server, and return the columns of the table
#'
#' @param server Name of SQL Server Instance
#' @param db Name of database
#' @param tbl Name of table
#' @return a vector contains columns of the table
#' @export
udf_GetColumns <- function(server, db, tbl) {
  query <- paste("select COLUMN_NAME as col_name from __db__.INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_NAME = '__tbl__'
                 AND COLUMN_NAME < > 'Default'
                 AND COLUMN_NAME < > 'CreatedDate'
                 AND COLUMN_NAME < > 'ModifiedDate'
                 AND DATA_TYPE not in ('image','ntext')")
  query <- gsub("__db__", db, gsub("__tbl__", tbl, query))
  query <- gsub("\n", "", query)
  queryout <- sqlQuery(server, query)
  res <- paste(as.character(queryout$col_name), collapse = ",")
  return(res)
}



#' Get a numerical unique identifier for a given table
#'
#' Takes in name of a table, database and the instance of a SQL Server, and return an unique identifer based on data and schema
#'
#' @param server Name of SQL Server Instance
#' @param db Name of database
#' @param tbl Name of table
#' @return value of the numerical unique identifer
#' @export
udf_GetChecksum <- function(server, db, tbl) {
  col_str <- udf_GetColumns(server,db,tbl)
  query <- paste("select ISNULL(CHECKSUM_AGG(CHECKSUM(__cols__)),0) from ", db, ".dbo.", tbl, sep = "")
  query <- gsub("__cols__", col_str, query)
  res <- sqlQuery(server, query)[1,1] ##ouput of sqlQuery() is a dataframe, need to slice down to vector, since we just need a number here
  return(res)
}



#' Get all the dependent tables for a stored procedure
#'
#' Takes in the name of a stored procedure, database and the instance of a SQL Server,
#'    and return the procedure's dependent tables
#'
#' @param server Name of SQL Server Instance
#' @param db Name of database
#' @param proc Name of stored procedure
#' @return a vector contains all dependent tables
#' @export
udf_GetTableList <- function(server,db, proc) {
  query <- "select b.referenced_entity_name as tbl_name
  from __db__.sys.objects a
  join __db__.sys.sql_expression_dependencies b
  on a.object_id = b.referencing_id
  join __db__.sys.objects c
  on c.object_id = b.referenced_id
  where a.name = '__proc__'
  and c.type = 'U'"
    query <- gsub("__proc__", proc, gsub("__db__", db, query))
    query <- gsub("\n", "", query)
    queryout <- sqlQuery(server, query)
    res <- as.character(queryout$tbl_name)
    return(res)
}



#' Get a dictionary of pairs of dependent tables and their identifier, for a given stored procedure
#'
#' Takes in the name of a stored procedure, database and the instance of a SQL Server,
#'     and return a dictionary of key values pairs of dependent tables and corresbonding identifier
#'
#' @param server Name of SQL Server Instance
#' @param db Name of database
#' @param proc Name of stored procedure
#' @return a dataframe contains dependent tables and their unique identifier
#' @export
udf_CreateChecksumDict <- function(server, db, proc) {
    tablist <- udf_GetTableList(server, db,proc)
    checksumlist <- sapply(tablist, udf_GetChecksum, server = server, db = db)
    res <- data.frame(tablist, checksumlist, stringsAsFactors = FALSE)
    return(res)
}



#' Merge the dictionary of source table and target table
#'
#' Takes in a source proc information and target proc information, and return a merged result of their dictionaries
#'
#' @param source_server Name of source SQL Server Instance
#' @param target_server Name of target SQL Server Instance
#' @param db Name of database
#' @param proc Name of stored procedure
#' @return a dataframe contains dependent tables, values of identifier on source server, and values on target server
#' @export
udf_MergeSourceTarget <- function(source_server, target_server, db, proc) {
    source <- udf_CreateChecksumDict(source_server, db, proc)
    target <- udf_CreateChecksumDict(target_server, db, proc)
    st_compare <- merge(x =source, y = target, by = 'tablist', all = TRUE)
    return(st_compare)  ##output is a dataframe
}



#' Compare a list of tables on source server and target server
#'
#' Takes in source server, target server, database and procedure, and return information on updated tables, new tables,
#'     missing tables and identical tables
#'
#' @param source_server Name of source SQL Server Instance
#' @param target_server Name of target SQL Server Instance
#' @param db Name of database
#' @param proc Name of stored procedure
#' @return a list contains updated tables, new tables, missing tables and identical tables
#' @export
udf_CompareSourceTarget <- function(source_server, target_server, db, proc) {
    m <- udf_MergeSourceTarget(source_server, target_server, db, proc)
    not_in_source <- m[is.na(m$checksumlist.x),]$tablist
    not_in_target <- m[is.na(m$checksumlist.y),]$tablist
    updated <- m[!is.na(m$checksumlist.x) & !is.na(m$checksumlist.y) & (m$checksumlist.x != m$checksumlist.y),]$tablist
    identical <- m[!is.na(m$checksumlist.x) & !is.na(m$checksumlist.y) & (m$checksumlist.x == m$checksumlist.y),]$tablist
    result <- list(not_in_source = not_in_source, not_in_target = not_in_target, updated = updated, identical=identical)
    return(result)
}
