% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Nasdaq.R
\name{Nasdaq}
\alias{Nasdaq}
\title{Nasdaq data}
\description{
Get data data from Nasdaq webpage.
}
\section{Methods}{
\subsection{Public methods}{
\itemize{
\item \href{#method-Nasdaq-ipo_calendar}{\code{Nasdaq$ipo_calendar()}}
\item \href{#method-Nasdaq-clone}{\code{Nasdaq$clone()}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-Nasdaq-ipo_calendar"></a>}}
\if{latex}{\out{\hypertarget{method-Nasdaq-ipo_calendar}{}}}
\subsection{Method \code{ipo_calendar()}}{
Get IPO calendar data.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Nasdaq$ipo_calendar(months = NULL)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{months}}{Months to get data for in %Y-%m format. If NULL, get data 
for all months.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
List with priced, filed and withdrawn IPOs data.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-Nasdaq-clone"></a>}}
\if{latex}{\out{\hypertarget{method-Nasdaq-clone}{}}}
\subsection{Method \code{clone()}}{
The objects of this class are cloneable with this method.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Nasdaq$clone(deep = FALSE)}\if{html}{\out{</div>}}
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
