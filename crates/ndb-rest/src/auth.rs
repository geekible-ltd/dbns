//! Bearer-token authentication middleware (spec §15.7, Phase-8 scope).
//!
//! Open-access default: when no tokens are configured every request maps to
//! `Role::Admin` (local/development mode per §15.8). Configured tokens map to
//! roles; admin endpoints require `Role::Admin` and are audit-logged via
//! `tracing` (§17.5 groundwork — full audit lands in Phase 10).

use axum::extract::{Request, State};
use axum::http::{StatusCode, header::AUTHORIZATION};
use axum::middleware::Next;
use axum::response::Response;
use std::sync::Arc;

/// Token → permission mapping. `None` fields = open access for that role.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// Read endpoints (GET) — `None` → open.
    pub reader: Option<String>,
    /// Write endpoints (POST/PUT/PATCH/DELETE) — `None` → open.
    pub writer: Option<String>,
    /// `/admin/*` + cluster endpoints — `None` → open.
    pub admin: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    Reader,
    Writer,
    Admin,
}

/// Shared auth state injected into the router.
pub type AuthState = Arc<AuthConfig>;

fn token_role(cfg: &AuthConfig, req: &Request) -> Option<Role> {
    let hdr = req
        .headers()
        .get(AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")?;
    if cfg.admin.as_deref() == Some(hdr) {
        Some(Role::Admin)
    } else if cfg.writer.as_deref() == Some(hdr) {
        Some(Role::Writer)
    } else if cfg.reader.as_deref() == Some(hdr) {
        Some(Role::Reader)
    } else {
        None
    }
}

fn required_role(_cfg: &AuthConfig, req: &Request) -> Role {
    let path = req.uri().path();
    if path.contains("/admin/") || path.contains("/cluster/") {
        return Role::Admin;
    }
    match req.method().as_str() {
        "GET" | "HEAD" => Role::Reader,
        _ => Role::Writer,
    }
}

/// Axum middleware enforcing the token map. Open access when nothing is
/// configured (§15.8 local/development default).
pub async fn auth_middleware(
    State(cfg): State<AuthState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let open = cfg.reader.is_none() && cfg.writer.is_none() && cfg.admin.is_none();
    if open {
        return Ok(next.run(req).await);
    }
    let need = required_role(&cfg, &req);
    let have = token_role(&cfg, &req).unwrap_or(match need {
        // reader endpoints stay open unless a reader token is configured.
        Role::Reader if cfg.reader.is_none() => Role::Reader,
        _ => return Err(StatusCode::UNAUTHORIZED),
    });
    if have < need {
        return Err(StatusCode::FORBIDDEN);
    }
    if need == Role::Admin {
        tracing::info!(path = %req.uri().path(), "admin endpoint accessed (audit)");
    }
    Ok(next.run(req).await)
}
