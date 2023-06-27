use std::time::Duration;

use axum::extract::rejection::TypedHeaderRejectionReason;
use axum::headers::{self, authorization};
use axum::response::IntoResponse;
use axum::TypedHeader;
use http::{request, HeaderValue, StatusCode};
use spacetimedb::auth::identity::{
    decode_token, encode_token, DecodingKey, EncodingKey, JwtError, SpacetimeIdentityClaims,
};
use spacetimedb::host::EnergyDiff;
use spacetimedb::identity::Identity;

use crate::{log_and_500, ControlNodeDelegate};

// Yes, this is using basic auth. See the below issues.
// The current form is: Authorization: Basic base64("token:<token>")
// FOOLS, the lot of them!
// If/when they fix this issue, this should be changed from
// basic auth, to a `Authorization: Bearer <token>` header
// https://github.com/whatwg/websockets/issues/16
// https://github.com/sta/websocket-sharp/pull/22
pub struct SpacetimeCreds(authorization::Basic);

const TOKEN_USERNAME: &str = "token";
impl authorization::Credentials for SpacetimeCreds {
    const SCHEME: &'static str = authorization::Basic::SCHEME;
    fn decode(value: &HeaderValue) -> Option<Self> {
        let basic = authorization::Basic::decode(value)?;
        if basic.username() != TOKEN_USERNAME {
            return None;
        }
        Some(Self(basic))
    }
    fn encode(&self) -> HeaderValue {
        self.0.encode()
    }
}

impl SpacetimeCreds {
    pub fn token(&self) -> &str {
        self.0.password()
    }
    pub fn decode_token(&self, public_key: &DecodingKey) -> Result<SpacetimeIdentityClaims, JwtError> {
        decode_token(public_key, self.token()).map(|x| x.claims)
    }
    pub fn encode_token(private_key: &EncodingKey, identity: Identity) -> Result<Self, JwtError> {
        let token = encode_token(private_key, identity)?;
        let headers::Authorization(basic) = headers::Authorization::basic(TOKEN_USERNAME, &token);
        Ok(Self(basic))
    }
}

pub struct SpacetimeAuth {
    pub creds: SpacetimeCreds,
    pub identity: Identity,
}

pub struct SpacetimeAuthHeader {
    auth: Option<SpacetimeAuth>,
}

#[async_trait::async_trait]
impl<S: ControlNodeDelegate + Send + Sync> axum::extract::FromRequestParts<S> for SpacetimeAuthHeader {
    type Rejection = AuthorizationRejection;
    async fn from_request_parts(parts: &mut request::Parts, state: &S) -> Result<Self, Self::Rejection> {
        match axum::TypedHeader::from_request_parts(parts, state).await {
            Ok(axum::TypedHeader(headers::Authorization(creds @ SpacetimeCreds { .. }))) => {
                let claims = creds
                    .decode_token(state.public_key())
                    .map_err(|_| AuthorizationRejection)?;
                let auth = SpacetimeAuth {
                    creds,
                    identity: claims.hex_identity,
                };
                Ok(Self { auth: Some(auth) })
            }
            Err(e) => match e.reason() {
                TypedHeaderRejectionReason::Missing => Ok(Self { auth: None }),
                _ => Err(AuthorizationRejection),
            },
        }
    }
}

pub struct AuthorizationRejection;
impl IntoResponse for AuthorizationRejection {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::BAD_REQUEST, "Authorization is invalid - malformed token.").into_response()
    }
}

impl SpacetimeAuth {
    pub async fn alloc(ctx: &(impl ControlNodeDelegate + ?Sized)) -> axum::response::Result<Self> {
        let identity = ctx.alloc_spacetime_identity().await.map_err(log_and_500)?;
        let creds = SpacetimeCreds::encode_token(ctx.private_key(), identity).map_err(log_and_500)?;
        Ok(Self { creds, identity })
    }

    pub fn into_headers(self) -> (TypedHeader<SpacetimeIdentity>, TypedHeader<SpacetimeIdentityToken>) {
        let Self { creds, identity } = self;
        (
            TypedHeader(SpacetimeIdentity(identity)),
            TypedHeader(SpacetimeIdentityToken(creds)),
        )
    }
}

impl SpacetimeAuthHeader {
    pub fn get(self) -> Option<SpacetimeAuth> {
        self.auth
    }

    /// Given an authorization header we will try to get the identity and token from the auth header (as JWT).
    /// If there is no JWT in the auth header we will create a new identity and token and return it.
    pub async fn get_or_create(
        self,
        ctx: &(impl ControlNodeDelegate + ?Sized),
    ) -> axum::response::Result<SpacetimeAuth> {
        match self.get() {
            Some(auth) => Ok(auth),
            None => SpacetimeAuth::alloc(ctx).await,
        }
    }
}

pub struct SpacetimeIdentity(pub Identity);
impl headers::Header for SpacetimeIdentity {
    fn name() -> &'static http::HeaderName {
        static NAME: http::HeaderName = http::HeaderName::from_static("spacetime-identity");
        &NAME
    }

    fn decode<'i, I: Iterator<Item = &'i HeaderValue>>(_values: &mut I) -> Result<Self, headers::Error> {
        unimplemented!()
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        values.extend([self.0.to_hex().try_into().unwrap()])
    }
}

pub struct SpacetimeIdentityToken(pub SpacetimeCreds);
impl headers::Header for SpacetimeIdentityToken {
    fn name() -> &'static http::HeaderName {
        static NAME: http::HeaderName = http::HeaderName::from_static("spacetime-identity-token");
        &NAME
    }

    fn decode<'i, I: Iterator<Item = &'i HeaderValue>>(_values: &mut I) -> Result<Self, headers::Error> {
        unimplemented!()
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        values.extend([self.0.token().try_into().unwrap()])
    }
}

pub struct SpacetimeEnergyUsed(pub EnergyDiff);
impl headers::Header for SpacetimeEnergyUsed {
    fn name() -> &'static http::HeaderName {
        static NAME: http::HeaderName = http::HeaderName::from_static("spacetime-energy-used");
        &NAME
    }

    fn decode<'i, I: Iterator<Item = &'i HeaderValue>>(_values: &mut I) -> Result<Self, headers::Error> {
        unimplemented!()
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        values.extend([self.0 .0.into()])
    }
}

pub struct SpacetimeExecutionDurationMicros(pub Duration);
impl headers::Header for SpacetimeExecutionDurationMicros {
    fn name() -> &'static http::HeaderName {
        static NAME: http::HeaderName = http::HeaderName::from_static("spacetime-execution-duration-micros");
        &NAME
    }

    fn decode<'i, I: Iterator<Item = &'i HeaderValue>>(_values: &mut I) -> Result<Self, headers::Error> {
        unimplemented!()
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        values.extend([(self.0.as_micros() as u64).into()])
    }
}