% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/MacroData.R
\name{MacroData}
\alias{MacroData}
\title{Macroeconomics Data}
\description{
Peovide macroeconomics data from various sources:
    - FRED
}
\section{Public fields}{
\if{html}{\out{<div class="r6-fields">}}
\describe{
\item{\code{path_to_dump}}{Local path to save data to.}

\item{\code{delay}}{Delay between requests.}
}
\if{html}{\out{</div>}}
}
\section{Methods}{
\subsection{Public methods}{
\itemize{
\item \href{#method-MacroData-new}{\code{MacroData$new()}}
\item \href{#method-MacroData-get_fred_metadata}{\code{MacroData$get_fred_metadata()}}
\item \href{#method-MacroData-get_alfred}{\code{MacroData$get_alfred()}}
\item \href{#method-MacroData-bulk_fred}{\code{MacroData$bulk_fred()}}
\item \href{#method-MacroData-clone}{\code{MacroData$clone()}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-MacroData-new"></a>}}
\if{latex}{\out{\hypertarget{method-MacroData-new}{}}}
\subsection{Method \code{new()}}{
Create a new MacroData object.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{MacroData$new(path_to_dump, delay = 0.8)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{path_to_dump}}{Local path to save data to.}

\item{\code{delay}}{Delay between requests.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
A new `MacroData` object.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-MacroData-get_fred_metadata"></a>}}
\if{latex}{\out{\hypertarget{method-MacroData-get_fred_metadata}{}}}
\subsection{Method \code{get_fred_metadata()}}{
Get FRED metadata.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{MacroData$get_fred_metadata(asset)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{asset}}{binance asset. Can be cm, um, option or spot}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
NULL.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-MacroData-get_alfred"></a>}}
\if{latex}{\out{\hypertarget{method-MacroData-get_alfred}{}}}
\subsection{Method \code{get_alfred()}}{
Get data from alfred.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{MacroData$get_alfred(id, vintage_dates, bin_len = 2000)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{id}}{Fred series id.}

\item{\code{vintage_dates}}{Vintage dates.}

\item{\code{bin_len}}{Bin length.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Data table with data.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-MacroData-bulk_fred"></a>}}
\if{latex}{\out{\hypertarget{method-MacroData-bulk_fred}{}}}
\subsection{Method \code{bulk_fred()}}{
Bulk FRED database.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{MacroData$bulk_fred(ids)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{ids}}{Character vector, Fred series ids.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
NULL.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-MacroData-clone"></a>}}
\if{latex}{\out{\hypertarget{method-MacroData-clone}{}}}
\subsection{Method \code{clone()}}{
The objects of this class are cloneable with this method.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{MacroData$clone(deep = FALSE)}\if{html}{\out{</div>}}
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
