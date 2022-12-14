% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/tabular.R
\name{query}
\alias{query}
\title{Query a datasource and returns an arrow Table}
\usage{
query(client, datasource, query, override = list())
}
\arguments{
\item{client}{As returned by \code{\link[=datasource_client]{datasource_client()}}}

\item{datasource}{The name of the datasource to query}

\item{query}{The query to run against the provided datasource}

\item{override}{Configuration values to override (\code{\link[=add_override]{add_override()}})}
}
\value{
An \link[arrow:Table]{arrow::Table}
}
\description{
Query a datasource and returns an arrow Table
}
