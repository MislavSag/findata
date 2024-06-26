% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/InvestingCom.R
\name{InvestingCom}
\alias{InvestingCom}
\title{InvestingCom Class}
\description{
Get data data from investing.com.
}
\section{Super class}{
\code{\link[findata:DataAbstract]{findata::DataAbstract}} -> \code{InvestingCom}
}
\section{Methods}{
\subsection{Public methods}{
\itemize{
\item \href{#method-InvestingCom-new}{\code{InvestingCom$new()}}
\item \href{#method-InvestingCom-get_investingcom_earnings_calendar_bulk}{\code{InvestingCom$get_investingcom_earnings_calendar_bulk()}}
\item \href{#method-InvestingCom-get_investingcom_earnings_calendar}{\code{InvestingCom$get_investingcom_earnings_calendar()}}
\item \href{#method-InvestingCom-update_investingcom_earnings}{\code{InvestingCom$update_investingcom_earnings()}}
\item \href{#method-InvestingCom-clone}{\code{InvestingCom$clone()}}
}
}
\if{html}{\out{
<details open><summary>Inherited methods</summary>
<ul>
<li><span class="pkg-link" data-pkg="findata" data-topic="DataAbstract" data-id="save_blob_files"><a href='../../findata/html/DataAbstract.html#method-DataAbstract-save_blob_files'><code>findata::DataAbstract$save_blob_files()</code></a></span></li>
</ul>
</details>
}}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-InvestingCom-new"></a>}}
\if{latex}{\out{\hypertarget{method-InvestingCom-new}{}}}
\subsection{Method \code{new()}}{
Create a new InvestingCom object.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{InvestingCom$new(azure_storage_endpoint = NULL, context_with_config = NULL)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{azure_storage_endpoint}}{Azure storate endpont}

\item{\code{context_with_config}}{AWS S3 Tiledb config}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
A new `FMP` object.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-InvestingCom-get_investingcom_earnings_calendar_bulk"></a>}}
\if{latex}{\out{\hypertarget{method-InvestingCom-get_investingcom_earnings_calendar_bulk}{}}}
\subsection{Method \code{get_investingcom_earnings_calendar_bulk()}}{
Get complete earnings calendar from investing.com.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{InvestingCom$get_investingcom_earnings_calendar_bulk(start_date)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{start_date}}{Start date}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Get investing com ea data.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-InvestingCom-get_investingcom_earnings_calendar"></a>}}
\if{latex}{\out{\hypertarget{method-InvestingCom-get_investingcom_earnings_calendar}{}}}
\subsection{Method \code{get_investingcom_earnings_calendar()}}{
Get Earnings Calendar from investing.com for specified dates
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{InvestingCom$get_investingcom_earnings_calendar(date_from, date_to)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{date_from}}{Start date}

\item{\code{date_to}}{End date}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Request to investing.com calendar
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-InvestingCom-update_investingcom_earnings"></a>}}
\if{latex}{\out{\hypertarget{method-InvestingCom-update_investingcom_earnings}{}}}
\subsection{Method \code{update_investingcom_earnings()}}{
Update earnings announcements data from investingcom website
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{InvestingCom$update_investingcom_earnings(path, start_date, update = TRUE)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{path}}{to save output DT.}

\item{\code{start_date}}{First date to scrape from.If NULL, takes last date from existing uri.}

\item{\code{update}}{existing uri.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Get and update ea data from investingcom,
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-InvestingCom-clone"></a>}}
\if{latex}{\out{\hypertarget{method-InvestingCom-clone}{}}}
\subsection{Method \code{clone()}}{
The objects of this class are cloneable with this method.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{InvestingCom$clone(deep = FALSE)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{deep}}{Whether to make a deep clone.}
}
\if{html}{\out{</div>}}
}
}
}
