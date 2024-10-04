#' @title CME Class
#'
#' @description
#' Get data data from CME data API.
#'
#' @export
CME = R6::R6Class(
  "CME",
  
  public = list(
    #' @field api_key API KEY for CME
    api_key = NULL,
    
    #' @field client_id Client ID for CME
    client_id = NULL,
    
    #' @field client_secret Client secret ID for CME
    client_secret = NULL,
    
    #' @field token Token we use to make requests
    token = NULL,
    
    #' @description
    #' Create a new CME object.
    #'
    #' @param api_key API KEY for CME cloud data.
    #' @param client_id Client ID for CME cloud data.
    #' @param client_secret Client Secret for CME cloud data.
    #'
    #' @return A new `CME` object.
    initialize = function(api_key = NULL, 
                          client_id = NULL, 
                          client_secret = NULL) {
      # Check and define api_key
      if (is.null(api_key)) {
        self$api_key = assert_character(Sys.getenv("APIKEY-CME"))
      } else {
        self$api_key = api_key
      }
      # Check and define client_id
      if (is.null(client_id)) {
        self$client_id = assert_character(Sys.getenv("CLIENTID-CME"))
      } else {
        self$client_id = client_id
      }
      # Check and define client_secret
      if (is.null(client_secret)) {
        self$client_secret = assert_character(Sys.getenv("CLIENTSECRET-CME"))
      } else {
        self$client_secret = client_secret
      }
      self$retoken()
    },
    
    #' @description
    #' Get CME token.
    #' 
    #' @return CME token.
    get_token = function() {
      auth_response = POST(
        private$auth_url,
        add_headers("Host"="auth.cmegroup.com"),
        body = list(
          grant_type = "client_credentials",
          client_id = self$client_id,
          client_secret = self$client_secret),
        encode = "form"
      )
      token_l = content(auth_response)
      return(token_l$access_token)
    },
    
    #' @description
    #' Refresh the authentication token if it's expired or missing.
    retoken = function() {
      if (is.null(self$token)) {
        self$token = self$get_token()
      }
    },
    
    #' @description
    #' Get the current FedWatch forecasts with full API parameter support.
    #'
    #' @param meeting_date Optional. Filter forecasts by specific meeting date in 'yyyy-mm-dd' format.
    #' @param reporting_date Optional. Filter forecasts by specific reporting date in 'yyyy-mm-dd' format.
    #' @param limit Optional. Limits the number of results returned. Default is 10.
    #'
    #' @return A list containing the FedWatch forecast data.
    fedwatch = function(meeting_date = NULL, reporting_date = NULL, limit = 10) {
      # Ensure the token is valid
      # self$retoken()
      
      # Prepare query parameters
      query_params = list(
        meetingDt = meeting_date,
        reportingDt = reporting_date,
        limit = limit
      )

      # Make the GET request to the endpoint
      response = GET(
        url = "https://markets.api.cmegroup.com/fedwatch/v1/forecasts",
        add_headers("Authorization" = paste("Bearer", self$token)),
        query = query_params
      )
      
      # Check if the request was successful
      if (response$status_code == 200) {
        return(content(response))
      } else {
        stop("Failed to retrieve FedWatch forecast. Status code: ", response$status_code)
      }
    },
    
    #' @description
    #' Get historical meetings data from the CME FedWatch API.
    #'
    #' @param limit An integer specifying the number of records to retrieve. Default is 10.
    #'
    #' @return A list containing the meetings history data.
    get_meetings_history = function(limit = 10) {
      # Ensure the token is valid
      # self$retoken()
      
      # Make the GET request to the endpoint
      response = GET(
        url = "https://markets.api.cmegroup.com/fedwatch/v1/meetings/history",
        add_headers("Authorization" = paste("Bearer", self$token)),
        query = list(limit = limit)
      )
      
      # Check if the request was successful
      if (response$status_code == 200) {
        return(content(response))
      } else {
        stop("Failed to retrieve meetings history. Status code: ", response$status_code)
      }
    }
  ),
  
  private = list(
    auth_url = "https://auth.cmegroup.com/as/token.oauth2"
  )
)
