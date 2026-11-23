use anyhow::Context;
use async_trait::async_trait;
use futures::{FutureExt, future::BoxFuture};
use http_body_util::BodyExt;
use http_body_util::Limited;
use reqwest::Body;
use reqwest::header::{AUTHORIZATION, USER_AGENT};
use reqwest::{Client, Request, RequestBuilder, Response, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use std::fmt::Debug;
use std::time::{Duration, SystemTime};
use tracing as log;
use url::Url;

use crate::github::IssueRepository;
use crate::jobs::Job;
use bytes::Bytes;
use http::{HeaderName, HeaderValue};
use serde::Serialize;

// TODO: Update to "2026-03-10" and see what breaks
// current version 2022-11-28 (supported until March 2028)
// Note: If you specify an API version that is no longer supported, you will receive a 410 Gone response.
// see: https://docs.github.com/rest/about-the-rest-api/api-versions?apiVersion=2026-03-10
const GITHUB_API_VERSION: &str = "2022-11-28";

/// Default repository used for API calls that are not attached to a specific repo.
pub(crate) fn default_issue_repository() -> IssueRepository {
    IssueRepository::new("rust-lang", "rust")
}

/// Finds the token in the user's environment, panicking if no suitable token
/// can be found.
pub fn default_token_from_env() -> SecretString {
    std::env::var("GITHUB_TOKEN")
        // kept for retrocompatibility but usage is discouraged and will be deprecated
        .or_else(|_| std::env::var("GITHUB_API_TOKEN"))
        .or_else(|_| get_token_from_git_config())
        .expect("could not find token in GITHUB_TOKEN, GITHUB_API_TOKEN or .gitconfig/github.oath-token")
        .into()
}

fn get_token_from_git_config() -> anyhow::Result<String> {
    let output = std::process::Command::new("git")
        .arg("config")
        .arg("--get")
        .arg("github.oauth-token")
        .output()?;
    if !output.status.success() {
        anyhow::bail!("error received executing `git`: {:?}", output.status);
    }
    let git_token = String::from_utf8(output.stdout)?.trim().to_string();
    Ok(git_token)
}

#[derive(Clone)]
pub struct GithubClient {
    token: SecretString,
    client: Client,
    api_url: Url,
    graphql_url: Url,
    raw_url: Url,
    /// If `true`, requests will sleep if it hits GitHub's rate limit.
    retry_rate_limit: bool,
}

impl GithubClient {
    pub fn new(token: SecretString, api_url: Url, graphql_url: Url, raw_url: Url) -> Self {
        GithubClient {
            client: Client::new(),
            token,
            api_url,
            graphql_url,
            raw_url,
            retry_rate_limit: false,
        }
    }

    pub fn new_from_env() -> Self {
        Self::new(
            default_token_from_env(),
            Url::parse(
                &std::env::var("GITHUB_API_URL")
                    .unwrap_or_else(|_| "https://api.github.com".to_string()),
            )
            .expect("couldn't parse GITHUB_API_URL"),
            Url::parse(
                &std::env::var("GITHUB_GRAPHQL_API_URL")
                    .unwrap_or_else(|_| "https://api.github.com/graphql".to_string()),
            )
            .expect("couldn't parse GITHUB_GRAPHQL_API_URL"),
            Url::parse(
                &std::env::var("GITHUB_RAW_URL")
                    .unwrap_or_else(|_| "https://raw.githubusercontent.com".to_string()),
            )
            .expect("couldn't parse GITHUB_RAW_URL"),
        )
    }

    pub(crate) fn api_url_with_segments<I>(&self, segments: I) -> Url
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let mut url = self.api_url.clone();
        url.path_segments_mut()
            .expect("api_url should have a base")
            .pop_if_empty()
            .extend(segments);
        url
    }

    pub(crate) fn raw_url_with_segments<I>(&self, segments: I) -> Url
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let mut url = self.raw_url.clone();
        url.path_segments_mut()
            .expect("raw_url should have a base")
            .pop_if_empty()
            .extend(segments);
        url
    }

    /// Sets whether or not this client will retry when it hits GitHub's rate limit.
    ///
    /// Just beware that the retry may take a long time (like 30 minutes,
    /// depending on various factors).
    pub fn set_retry_rate_limit(&mut self, retry: bool) {
        self.retry_rate_limit = retry;
    }

    pub fn raw(&self) -> &Client {
        &self.client
    }

    /// Prepares the request for being sent out.
    /// Configures authentication for it.
    pub(crate) fn prepare_request(&self, req: GithubRequest) -> anyhow::Result<(Request, String)> {
        let GithubRequest { url: _, request } = req;
        let req_dbg = format!("{request:?}");
        let request = request.configure(self);
        let req = request
            .build()
            .with_context(|| format!("failed to build request {req_dbg:?}"))?;
        Ok((req, req_dbg))
    }

    pub async fn send_req(&self, req: GithubRequest) -> anyhow::Result<(Bytes, String)> {
        const MAX_DEFAULT_RESPONSE_SIZE: usize = 8 * 1024 * 1024; // 8 Mib

        self.send_req_with_limit(req, MAX_DEFAULT_RESPONSE_SIZE)
            .await
    }

    pub async fn send_req_with_limit(
        &self,
        req: GithubRequest,
        max_response_size: usize,
    ) -> anyhow::Result<(Bytes, String)> {
        const MAX_ATTEMPTS: u32 = 2;

        log::debug!("send_req with {:?}", req);

        let (req, req_dbg) = self.prepare_request(req)?;
        let req_url = req.url().to_string();

        let mut resp = self.client.execute(req.try_clone().unwrap()).await?;
        if self.retry_rate_limit
            && let Some(sleep) = Self::needs_retry(&resp).await
        {
            resp = self.retry(req, sleep, MAX_ATTEMPTS).await?;
        }

        let maybe_err = resp.error_for_status_ref().err();
        let github_request_id = resp.headers().get("x-github-request-id").cloned();

        let resp: http::Response<Body> = resp.into();
        let limited = Limited::new(resp, max_response_size);

        let body = match limited.collect().await {
            Ok(body) => body.to_bytes(),
            Err(e) => match e.downcast::<http_body_util::LengthLimitError>() {
                Ok(e) => {
                    return Err(anyhow::Error::new(*e)).with_context(|| {
                            format!(
                                "req={req_url} (x-github-request-id: {}): max response size exceeded (over {max_response_size} bytes)",
                                github_request_id
                                    .as_ref()
                                    .and_then(|v| v.to_str().ok())
                                    .unwrap_or("unknown")
                            )
                        });
                }
                Err(e) => {
                    return Err(anyhow::Error::from_boxed(e)).with_context(|| {
                            format!(
                                "req={req_url} (x-github-request-id: {}): unable to complete the request",
                                github_request_id
                                    .as_ref()
                                    .and_then(|v| v.to_str().ok())
                                    .unwrap_or("unknown")
                            )
                        });
                }
            },
        };

        if let Some(e) = maybe_err {
            return Err(anyhow::Error::new(e)).with_context(|| {
                format!(
                    "req={req_url} (x-github-request-id: {}): {:.500}",
                    github_request_id
                        .as_ref()
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("unknown"),
                    String::from_utf8_lossy(&body),
                )
            });
        }

        Ok((body, req_dbg))
    }

    async fn needs_retry(resp: &Response) -> Option<Duration> {
        const REMAINING: &str = "X-RateLimit-Remaining";
        const RESET: &str = "X-RateLimit-Reset";

        if !matches!(
            resp.status(),
            StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS
        ) {
            return None;
        }

        let headers = resp.headers();
        if !(headers.contains_key(REMAINING) && headers.contains_key(RESET)) {
            return None;
        }

        let reset_time = headers[RESET].to_str().unwrap().parse::<u64>().unwrap();
        Some(Duration::from_secs(Self::calc_sleep(reset_time) + 10))
    }

    fn calc_sleep(reset_time: u64) -> u64 {
        let epoch_time = SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs();
        reset_time.saturating_sub(epoch_time)
    }

    fn retry(
        &self,
        req: Request,
        sleep: Duration,
        remaining_attempts: u32,
    ) -> BoxFuture<'_, Result<Response, reqwest::Error>> {
        #[derive(Debug, serde::Deserialize)]
        struct RateLimitResponse {
            resources: RateLimitResources,
        }

        log::warn!(
            "Retrying after {} seconds, remaining attepts {}",
            sleep.as_secs(),
            remaining_attempts,
        );

        async move {
            tokio::time::sleep(sleep).await;

            // check rate limit
            let rate_resp = self
                .client
                .execute(
                    self.client
                        .get(format!("{}/rate_limit", self.api_url))
                        .configure(self)
                        .build()
                        .unwrap(),
                )
                .await?;
            rate_resp.error_for_status_ref()?;
            let rate_limit_response = rate_resp.json::<RateLimitResponse>().await?;

            // Check url for search path because github has different rate limits for the search api
            let rate_limit = if req
                .url()
                .path_segments()
                .is_some_and(|mut segments| segments.next() == Some("search"))
            {
                rate_limit_response.resources.search
            } else {
                rate_limit_response.resources.core
            };

            // If we still don't have any more remaining attempts, try sleeping for the remaining
            // period of time
            if rate_limit.remaining == 0 {
                let sleep = Self::calc_sleep(rate_limit.reset);
                if sleep > 0 {
                    tokio::time::sleep(Duration::from_secs(sleep)).await;
                }
            }

            let resp = self.client.execute(req.try_clone().unwrap()).await?;
            if let Some(sleep) = Self::needs_retry(&resp).await
                && remaining_attempts > 0
            {
                return self.retry(req, sleep, remaining_attempts - 1).await;
            }

            Ok(resp)
        }
        .boxed()
    }

    pub async fn json<T>(&self, req: GithubRequest) -> anyhow::Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let (body, _req_dbg) = self.send_req(req).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub fn get(&self, url: GithubUrl) -> GithubRequest {
        log::trace!("get {:?}", url);
        GithubRequest::new(url.clone(), self.client.get(url.url().clone()))
    }

    pub fn patch(&self, url: GithubUrl) -> GithubRequest {
        log::trace!("patch {:?}", url);
        GithubRequest::new(url.clone(), self.client.patch(url.url().clone()))
    }

    pub fn delete(&self, url: GithubUrl) -> GithubRequest {
        log::trace!("delete {:?}", url);
        GithubRequest::new(url.clone(), self.client.delete(url.url().clone()))
    }

    pub fn post(&self, url: GithubUrl) -> GithubRequest {
        log::trace!("post {:?}", url);
        GithubRequest::new(url.clone(), self.client.post(url.url().clone()))
    }

    pub fn put(&self, url: GithubUrl) -> GithubRequest {
        log::trace!("put {:?}", url);
        GithubRequest::new(url.clone(), self.client.put(url.url().clone()))
    }

    /// Fetch current rate limit, remaining and used, for the given repository.
    pub async fn rate_limit(&self, repo: &IssueRepository) -> anyhow::Result<RateLimitResources> {
        #[derive(Debug, serde::Deserialize)]
        struct RateLimitResponse {
            resources: RateLimitResources,
        }

        let url = self.api_url_with_segments(["rate_limit"]);
        let response = self
            .json::<RateLimitResponse>(self.get(GithubUrl::new(repo, url)))
            .await
            .context("failed to fetch GitHub /rate_limit endpoint")?;

        Ok(response.resources)
    }

    /// Issues an ad-hoc GraphQL query.
    ///
    /// You are responsible for checking the `errors` array when calling this
    /// function to determine if there is an error. Only use this if you are
    /// looking for specific error codes, or don't care about errors. Use
    /// [`GithubClient::graphql_query`] if you would prefer to have a generic
    /// error message.
    pub async fn graphql_query_with_errors(
        &self,
        repo: &IssueRepository,
        query: &str,
        vars: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        // Our GraphQl query can end-up being quite big, let's set a higher default
        // response size than for normal REST Api response.
        const MAX_DEFAULT_GRAPH_QL_RESPONSE_SIZE: usize = 10 * 1024 * 1024;

        let (body, _dbg) = self
            .send_req_with_limit(
                self.post(GithubUrl::new(repo, self.graphql_url.clone()))
                    .json(&serde_json::json!({
                        "query": query,
                        "variables": vars,
                    })),
                MAX_DEFAULT_GRAPH_QL_RESPONSE_SIZE,
            )
            .await?;

        Ok(serde_json::from_slice(&body)?)
    }

    /// Issues an ad-hoc GraphQL query.
    ///
    /// See [`GithubClient::graphql_query_with_errors`] if you need to check
    /// for specific errors.
    pub async fn graphql_query(
        &self,
        repo: &IssueRepository,
        query: &str,
        vars: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let mut result: serde_json::Value =
            self.graphql_query_with_errors(repo, query, vars).await?;
        if let Some(errors) = result["errors"].take().as_array_mut() {
            anyhow::bail!(GraphQlErrors {
                errors: std::mem::take(errors)
                    .into_iter()
                    .map(|err| serde_json::from_value(err).unwrap_or_default())
                    .collect(),
            })
        }
        Ok(result)
    }
}

#[derive(Debug)]
pub struct GithubRequest {
    url: GithubUrl,
    request: RequestBuilder,
}

impl GithubRequest {
    pub(crate) fn new(url: GithubUrl, request: RequestBuilder) -> Self {
        Self { url, request }
    }

    pub(crate) fn query<T: Serialize + ?Sized>(self, value: &T) -> Self {
        Self {
            url: self.url,
            request: self.request.query(value),
        }
    }

    pub(crate) fn header<K, V>(self, key: K, value: V) -> Self
    where
        HeaderName: TryFrom<K>,
        <HeaderName as TryFrom<K>>::Error: Into<http::Error>,
        HeaderValue: TryFrom<V>,
        <HeaderValue as TryFrom<V>>::Error: Into<http::Error>,
    {
        Self {
            url: self.url,
            request: self.request.header(key, value),
        }
    }

    pub(crate) fn json<T: Serialize>(self, value: &T) -> Self {
        Self {
            url: self.url,
            request: self.request.json(value),
        }
    }
}

#[derive(Clone, Debug)]
pub struct GithubUrl {
    url: Url,
    repo: IssueRepository,
}

impl GithubUrl {
    pub(crate) fn new(repo: &IssueRepository, url: Url) -> Self {
        Self {
            url,
            repo: repo.clone(),
        }
    }

    pub(crate) fn generic(url: Url) -> Self {
        Self::new(&default_issue_repository(), url)
    }

    pub(crate) fn url(&self) -> &Url {
        &self.url
    }

    pub(crate) fn map_url<F>(self, map_url: F) -> Self
    where
        F: FnOnce(Url) -> Url,
    {
        Self {
            url: map_url(self.url),
            repo: self.repo,
        }
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct RateLimitResources {
    pub core: RateLimit,
    pub search: RateLimit,
    pub graphql: RateLimit,
}

#[derive(Debug, serde::Deserialize)]
pub struct RateLimit {
    pub limit: u64,
    pub remaining: u64,
    pub reset: u64,
}

#[derive(Debug)]
pub struct GraphQlErrors {
    pub errors: Vec<GraphQlError>,
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct GraphQlError {
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub path: Vec<String>,
    #[serde(default, rename = "type")]
    pub type_: String,
}

impl std::fmt::Display for GraphQlErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, err) in self.errors.iter().enumerate() {
            if i > 0 {
                f.write_str("\n")?;
            }
            f.write_str(&err.message)?;
        }
        Ok(())
    }
}

trait RequestSend: Sized {
    fn configure(self, g: &GithubClient) -> Self;
}

impl RequestSend for RequestBuilder {
    fn configure(self, g: &GithubClient) -> RequestBuilder {
        let mut auth = reqwest::header::HeaderValue::from_maybe_shared(format!(
            "token {}",
            g.token.expose_secret()
        ))
        .unwrap();
        auth.set_sensitive(true);
        self.header(USER_AGENT, "rust-lang-triagebot")
            .header(AUTHORIZATION, &auth)
            .header("X-GitHub-Api-Version", GITHUB_API_VERSION)
    }
}

// Rate limit logging job

pub struct GithubRateLimitLoggingJob;

#[async_trait]
impl Job for GithubRateLimitLoggingJob {
    fn name(&self) -> &'static str {
        "rate_limit_logging_job"
    }

    async fn run(
        &self,
        ctx: &crate::handlers::Context,
        _metadata: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let rates = match ctx.github.rate_limit(&default_issue_repository()).await {
            Ok(rates) => rates,
            Err(err) => {
                tracing::error!("failed to fetch the current rate limits for Github: {err:?}");
                return Ok(());
            }
        };

        tracing::info!(
            "Github rate_limit: core={}/{} search={}/{} graphql={}/{}",
            rates.core.remaining,
            rates.core.limit,
            rates.search.remaining,
            rates.search.limit,
            rates.graphql.remaining,
            rates.graphql.limit
        );

        Ok(())
    }
}

#[test]
fn gh_api_version() {
    let c = GithubClient {
        client: Client::new(),
        token: String::new().into(),
        api_url: Url::parse("https://api.github.com").unwrap(),
        graphql_url: Url::parse("https://api.github.com/graphql").unwrap(),
        raw_url: Url::parse("https://raw.githubusercontent.com").unwrap(),
        retry_rate_limit: false,
    };

    let request = c
        .prepare_request(c.get(GithubUrl::generic(
            Url::parse("http://www.example.com").unwrap(),
        )))
        .unwrap();
    let headers = request
        .0
        .headers()
        .iter()
        .filter(|h| h.0 == "x-github-api-version")
        .map(|l| l.1)
        .collect::<Vec<_>>();
    assert_eq!(headers[0].to_str().unwrap(), "2022-11-28");
}
