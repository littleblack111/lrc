use futures::stream::{iter, StreamExt};
use lofty::{
    prelude::{Accessor, AudioFile, TaggedFileExt},
    probe::Probe,
};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::{
    env,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs,
    time::{sleep, Duration},
};
use walkdir::WalkDir;

#[derive(Deserialize, Debug)]
struct LrcLibResponse {
    #[serde(rename = "syncedLyrics")]
    synced_lyrics: Option<String>,
    #[serde(rename = "plainLyrics")]
    plain_lyrics: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!(
            "Usage: {} <directory>",
            args[0]
        );
        std::process::exit(1);
    }
    let dir = &args[1];
    let mut files = Vec::new();

    for entry in WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if path.is_file() {
            if let Some(ext) = path
                .extension()
                .and_then(|s| s.to_str())
            {
                if [
                    "flac", "mp3", "m4a", "ogg", "wav",
                ]
                .contains(
                    &ext.to_lowercase()
                        .as_str(),
                ) {
                    if !path
                        .with_extension("lrc")
                        .exists()
                    {
                        files.push(path.to_path_buf());
                    }
                }
            }
        }
    }

    let client = Arc::new(
        Client::builder()
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
            .build()?,
    );

    let token_url =
        "https://apic-desktop.musixmatch.com/ws/1.1/token.get?app_id=web-desktop-app-v1.0";
    let token = if let Ok(resp) = client
        .get(token_url)
        .send()
        .await
    {
        if let Ok(json) = resp
            .json::<Value>()
            .await
        {
            json["message"]["body"]["user_token"]
                .as_str()
                .unwrap_or("26050189b284b4711e09e7eced106bf59516579e537950496a89fd")
                .to_string()
        } else {
            "26050189b284b4711e09e7eced106bf59516579e537950496a89fd".to_string()
        }
    } else {
        "26050189b284b4711e09e7eced106bf59516579e537950496a89fd".to_string()
    };

    let token = Arc::new(token);

    let stream = iter(files).map(
        |path| {
            let client = Arc::clone(&client);
            let token = Arc::clone(&token);
            async move {
                process_file(
                    client, token, path,
                )
                .await;
            }
        },
    );

    stream
        .buffer_unordered(1)
        .collect::<Vec<()>>()
        .await;
    Ok(())
}

async fn process_file(client: Arc<Client>, token: Arc<String>, path: PathBuf) {
    let Some((mut title, artist, _album, _duration)) = extract_metadata(&path) else {
        return;
    };
    let filename = path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    title = title
        .replace(
            " [Explicit]",
            "",
        )
        .replace(
            "[Explicit]",
            "",
        );

    if let Some(enhanced) = fetch_musixmatch_enhanced(
        &client,
        &token,
        &title,
        Some(&artist),
    )
    .await
    {
        save_lrc(
            &path,
            &enhanced,
            "Enhanced LRC",
            &filename,
        )
        .await;
        return;
    }

    sleep(Duration::from_millis(500)).await;

    if let Some(enhanced) = fetch_musixmatch_enhanced(
        &client, &token, &title, None,
    )
    .await
    {
        save_lrc(
            &path,
            &enhanced,
            "Enhanced LRC",
            &filename,
        )
        .await;
        return;
    }

    let title_enc = urlencoding::encode(&title);
    let artist_enc = urlencoding::encode(&artist);
    let url = format!(
        "https://lrclib.net/api/search?track_name={}&artist_name={}",
        title_enc, artist_enc
    );
    if let Ok(response) = client
        .get(&url)
        .send()
        .await
    {
        if response
            .status()
            .is_success()
        {
            if let Ok(results) = response
                .json::<Vec<LrcLibResponse>>()
                .await
            {
                for res in &results {
                    if let Some(synced) = &res.synced_lyrics {
                        if !synced
                            .trim()
                            .is_empty()
                        {
                            save_lrc(
                                &path,
                                synced,
                                "Standard LRC",
                                &filename,
                            )
                            .await;
                            return;
                        }
                    }
                }
                for res in &results {
                    if let Some(plain) = &res.plain_lyrics {
                        if !plain
                            .trim()
                            .is_empty()
                        {
                            save_lrc(
                                &path,
                                plain,
                                "Plain text",
                                &filename,
                            )
                            .await;
                            return;
                        }
                    }
                }
            }
        }
    }

    println!(
        "Not found: {} - {}",
        title, artist
    );
}

async fn fetch_musixmatch_enhanced(
    client: &Client,
    token: &str,
    title: &str,
    artist: Option<&str>,
) -> Option<String> {
    let base_url = "https://apic-desktop.musixmatch.com/ws/1.1/track.search";

    let query = if let Some(a) = artist {
        format!(
            "q_track={}&q_artist={}",
            urlencoding::encode(title),
            urlencoding::encode(a)
        )
    } else {
        format!(
            "q_track={}",
            urlencoding::encode(title)
        )
    };

    let url = format!(
        "{}?{}&s_track_rating=desc&page_size=1&page=1&app_id=web-desktop-app-v1.0&usertoken={}",
        base_url, query, token
    );

    if let Ok(response) = client
        .get(&url)
        .send()
        .await
    {
        if let Ok(json) = response
            .json::<Value>()
            .await
        {
            if let Some(track_list) = json["message"]["body"]["track_list"].as_array() {
                for item in track_list {
                    if let Some(track_id) = item["track"]["track_id"].as_u64() {
                        sleep(Duration::from_millis(500)).await;

                        let rs_url = format!("https://apic-desktop.musixmatch.com/ws/1.1/track.richsync.get?track_id={}&app_id=web-desktop-app-v1.0&usertoken={}", track_id, token);
                        if let Ok(rs_resp) = client
                            .get(&rs_url)
                            .send()
                            .await
                        {
                            if let Ok(rs_json) = rs_resp
                                .json::<Value>()
                                .await
                            {
                                if rs_json["message"]["header"]["status_code"].as_u64() == Some(200)
                                {
                                    if let Some(body_str) = rs_json["message"]["body"]["richsync"]
                                        ["richsync_body"]
                                        .as_str()
                                    {
                                        if let Ok(lrc_raw) =
                                            serde_json::from_str::<Vec<Value>>(body_str)
                                        {
                                            let mut lrc_str = String::new();
                                            for line in lrc_raw {
                                                if let Some(ts) = line["ts"].as_f64() {
                                                    lrc_str.push_str(
                                                        &format!(
                                                            "[{}] ",
                                                            format_time(ts)
                                                        ),
                                                    );
                                                    if let Some(words) = line["l"].as_array() {
                                                        for word in words {
                                                            if let (Some(o), Some(c)) = (
                                                                word["o"].as_f64(),
                                                                word["c"].as_str(),
                                                            ) {
                                                                let t = ts + o;
                                                                lrc_str.push_str(
                                                                    &format!(
                                                                        "<{}>{} ",
                                                                        format_time(t),
                                                                        c
                                                                    ),
                                                                );
                                                            }
                                                        }
                                                    }
                                                    lrc_str.push('\n');
                                                }
                                            }
                                            return Some(lrc_str);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

fn format_time(mut seconds: f64) -> String {
    let mins = (seconds / 60.0).floor() as u64;
    seconds = seconds % 60.0;
    format!(
        "{:02}:{:05.2}",
        mins, seconds
    )
}

async fn save_lrc(path: &Path, lyrics: &str, lrc_type: &str, filename: &str) {
    let lrc_path = path.with_extension("lrc");
    let _ = fs::write(
        &lrc_path, lyrics,
    )
    .await;
    println!(
        "Saved {} for {}",
        lrc_type, filename
    );
}

fn extract_metadata(
    path: &Path,
) -> Option<(
    String,
    String,
    String,
    u64,
)> {
    let tagged_file = Probe::open(path)
        .ok()?
        .read()
        .ok()?;
    let tag = tagged_file
        .primary_tag()
        .or_else(|| tagged_file.first_tag())?;

    let title = tag
        .title()
        .map(|s| s.into_owned())?;
    let artist = tag
        .artist()
        .map(|s| s.into_owned())?;
    let album = tag
        .album()
        .map(|s| s.into_owned())
        .unwrap_or_default();
    let duration = tagged_file
        .properties()
        .duration()
        .as_secs();

    Some(
        (
            title, artist, album, duration,
        ),
    )
}
