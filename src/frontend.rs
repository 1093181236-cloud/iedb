// Embedded frontend assets. Shared by agent (serves agent.html at GET /)
// and server (serves index.html at GET /).
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "frontend/"]
struct Assets;

/// Serve an embedded file. `default_page` is returned when `path` is "/"
/// or empty (e.g. "agent.html" for agent mode, "index.html" for server mode).
pub fn serve(default_page: &str, path: &str) -> Option<(String, &'static str)> {
    let path = path.trim_start_matches('/');
    let path = if path.is_empty() { default_page } else { path };
    Assets::get(path).map(|f| {
        let mime = if path.ends_with(".html") { "text/html; charset=utf-8" }
              else if path.ends_with(".js") { "application/javascript" }
              else if path.ends_with(".css") { "text/css" }
              else { "application/octet-stream" };
        (String::from_utf8_lossy(&f.data).into_owned(), mime)
    })
}
