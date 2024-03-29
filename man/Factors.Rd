% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Factors.R
\name{Factors}
\alias{Factors}
\title{Factors Class}
\description{
Function calculates factor often (and less often) used in asset pricing
factor investing literature.
}
\section{Super class}{
\code{\link[findata:DataAbstract]{findata::DataAbstract}} -> \code{Factors}
}
\section{Public fields}{
\if{html}{\out{<div class="r6-fields">}}
\describe{
\item{\code{source}}{Data source for calculating factors.}

\item{\code{fmp}}{Help field to hold FMP class.}

\item{\code{import}}{Help field to hold Import class.}

\item{\code{utilsdata}}{Help field to hold Import class.}

\item{\code{sp500_symbols}}{Path to QuantConnect SPY file.}
}
\if{html}{\out{</div>}}
}
\section{Methods}{
\subsection{Public methods}{
\itemize{
\item \href{#method-Factors-new}{\code{Factors$new()}}
\item \href{#method-Factors-get_factors}{\code{Factors$get_factors()}}
\item \href{#method-Factors-clone}{\code{Factors$clone()}}
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
\if{html}{\out{<a id="method-Factors-new"></a>}}
\if{latex}{\out{\hypertarget{method-Factors-new}{}}}
\subsection{Method \code{new()}}{
Create a new Factors object.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Factors$new(sp500_symbols, source = "fmp", fredr_apikey = NULL)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{sp500_symbols}}{Path to QuantConnect SPY file.}

\item{\code{source}}{Data source for calculating factors.}

\item{\code{fredr_apikey}}{FRED api key}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
A new `Factors` object.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-Factors-get_factors"></a>}}
\if{latex}{\out{\hypertarget{method-Factors-get_factors}{}}}
\subsection{Method \code{get_factors()}}{
Main function that calculates factors.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Factors$get_factors(...)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{...}}{Arguemnts of get_data_fmp method of Import class.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Data.table with factors.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-Factors-clone"></a>}}
\if{latex}{\out{\hypertarget{method-Factors-clone}{}}}
\subsection{Method \code{clone()}}{
The objects of this class are cloneable with this method.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Factors$clone(deep = FALSE)}\if{html}{\out{</div>}}
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
