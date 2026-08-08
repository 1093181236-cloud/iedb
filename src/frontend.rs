// Embedded frontend assets served at GET /.
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "frontend/"]
struct Assets;

/// Serve the embedded index.html or return 404 for unknown paths.
pub fn serve(path: &str) -> Option<(String, &'static str)> {
    let path = path.trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    Assets::get(path).map(|f| {
        let mime = if path.ends_with(".html") { "text/html; charset=utf-8" }
              else if path.ends_with(".js") { "application/javascript" }
              else if path.ends_with(".css") { "text/css" }
              else { "application/octet-stream" };
        (String::from_utf8_lossy(&f.data).into_owned(), mime)
    })
}
