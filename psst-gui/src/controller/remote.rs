//! Musicaplay: plays through the official Spotify app over Spotify Connect.
//!
//! Spotify stopped giving audio keys to librespot for most accounts, so
//! Musicaplay can no longer decode tracks itself. Instead it sends what to
//! play to a Spotify Connect device, normally the official app on this
//! computer, and reads back what plays to keep its own view current.
//!
//! Every Web API call blocks, so they all run on one worker thread; the UI
//! thread only sends commands. Failures never panic: they come back to the UI
//! as an alert.

use std::{thread, time::Duration};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use druid::{ExtEventSink, Selector, Target, WidgetId};
use psst_core::item_id::{ItemId, ItemIdType};

use crate::{cmd, webapi::WebApi};

/// A message for the person, shown as an error alert.
pub const REMOTE_ERROR: Selector<String> = Selector::new("musicaplay.remote-error");

/// How often what plays is read back.
const POLL: Duration = Duration::from_secs(1);
/// At most this many tracks go to Spotify in one play request.
const MOST_URIS: usize = 500;
/// Tracks before the chosen one that go too, so Previous still works.
const BEFORE: usize = 100;

pub enum RemoteCommand {
    /// Plays these URIs, starting at `position` among them.
    Play { uris: Vec<String>, position: usize },
    Pause,
    Resume,
    Toggle,
    Next,
    Previous,
    Seek(Duration),
    /// 0.0 to 1.0.
    Volume(f64),
    Queue(String),
}

impl RemoteCommand {
    fn name(&self) -> &'static str {
        match self {
            Self::Play { .. } => "play",
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::Toggle => "toggle",
            Self::Next => "next",
            Self::Previous => "previous",
            Self::Seek(_) => "seek",
            Self::Volume(_) => "volume",
            Self::Queue(_) => "queue",
        }
    }
}

pub struct Remote {
    sender: Sender<RemoteCommand>,
}

impl Remote {
    pub fn start(event_sink: ExtEventSink, widget_id: WidgetId) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let spawned = thread::Builder::new()
            .name("musicaplay-remote".into())
            .spawn(move || Worker::new(event_sink, widget_id).run(receiver));
        if let Err(error) = spawned {
            log::error!("could not start the remote worker: {error}");
        }
        Self { sender }
    }

    pub fn send(&self, command: RemoteCommand) {
        log::info!("remote: sending {}", command.name());
        if let Err(error) = self.sender.send(command) {
            log::error!("remote worker is gone: {error}");
        }
    }

    /// The Web API URI of an item, or None for a local file.
    pub fn uri(item: &ItemId) -> Option<String> {
        item.to_uri()
            .map(|uri| uri.replacen("spotify:podcast:", "spotify:episode:", 1))
    }

    /// The URIs of `items` to send, at most [`MOST_URIS`] around `position`,
    /// and where the chosen one is among them; local files are left out.
    pub fn window(items: &[ItemId], position: usize) -> (Vec<String>, usize) {
        let start = position.saturating_sub(BEFORE);
        let mut uris = Vec::new();
        let mut chosen = 0;
        for (index, item) in items.iter().enumerate().skip(start) {
            if uris.len() == MOST_URIS {
                break;
            }
            if index == position {
                chosen = uris.len();
            }
            if let Some(uri) = Self::uri(item) {
                uris.push(uri);
            }
        }
        (uris, chosen)
    }
}

/// What was last shown, to send the UI only changes.
#[derive(Default, PartialEq)]
struct Shown {
    item: Option<ItemId>,
    playing: bool,
}

struct Worker {
    event_sink: ExtEventSink,
    widget_id: WidgetId,
    device: Option<String>,
    shown: Shown,
    /// Whether Spotify said anything plays at the last poll.
    playing: bool,
    /// Whether the last poll failed, to alert once rather than every second.
    poll_failed: bool,
}

impl Worker {
    fn new(event_sink: ExtEventSink, widget_id: WidgetId) -> Self {
        Self {
            event_sink,
            widget_id,
            device: None,
            shown: Shown::default(),
            playing: false,
            poll_failed: false,
        }
    }

    fn run(mut self, receiver: Receiver<RemoteCommand>) {
        loop {
            match receiver.recv_timeout(POLL) {
                Ok(command) => self.command(command),
                Err(RecvTimeoutError::Timeout) => self.poll(),
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    fn submit<T: Send + 'static>(&self, selector: Selector<T>, payload: T) {
        if let Err(error) =
            self.event_sink
                .submit_command(selector, payload, Target::Widget(self.widget_id))
        {
            log::error!("could not reach the UI: {error}");
        }
    }

    fn alert(&self, message: impl Into<String>) {
        self.submit(REMOTE_ERROR, message.into());
    }

    /// The device to play on: the one already playing, else the official app
    /// on a computer, else any. None, with an alert, when there is none.
    fn device(&mut self) -> Option<String> {
        match WebApi::global().remote_devices() {
            Ok(devices) => {
                log::info!(
                    "remote: {} Spotify Connect devices: {:?}",
                    devices.len(),
                    devices
                        .iter()
                        .map(|device| format!("{} ({}, active: {})", device.name, device.kind, device.is_active))
                        .collect::<Vec<_>>()
                );
                let chosen = devices
                    .iter()
                    .find(|device| device.is_active)
                    .or_else(|| {
                        devices
                            .iter()
                            .find(|device| device.kind.eq_ignore_ascii_case("computer"))
                    })
                    .or_else(|| devices.first())
                    .and_then(|device| device.id.clone());
                if chosen.is_none() {
                    self.alert("Ouvre l'appli Spotify officielle (même réduite) : c'est elle qui joue le son.");
                }
                self.device = chosen.clone();
                chosen
            }
            Err(error) => {
                log::warn!("could not list Spotify devices: {error}");
                self.alert("Impossible de joindre Spotify. Vérifie ta connexion.");
                None
            }
        }
    }

    fn command(&mut self, command: RemoteCommand) {
        // Play looks for a device each time, as the app may have been opened
        // since; other commands use the one found before when there is one.
        let look = matches!(command, RemoteCommand::Play { .. }) || self.device.is_none();
        let device = if look {
            self.device()
        } else {
            self.device.clone()
        };
        let Some(device) = device else {
            log::warn!("remote: no device to play on");
            return;
        };
        log::info!("remote: {} on device {device}", command.name());
        let device = Some(device.as_str());
        let api = WebApi::global();
        let result = match command {
            RemoteCommand::Play { uris, position } => {
                if uris.is_empty() {
                    self.alert("Rien à jouer ici (les fichiers locaux ne passent pas par Spotify).");
                    return;
                }
                api.remote_play(device, &uris, position)
            }
            RemoteCommand::Pause => api.remote_pause(device),
            RemoteCommand::Resume => api.remote_resume(device),
            RemoteCommand::Toggle if self.playing => api.remote_pause(device),
            RemoteCommand::Toggle => api.remote_resume(device),
            RemoteCommand::Next => api.remote_next(device),
            RemoteCommand::Previous => api.remote_previous(device),
            RemoteCommand::Seek(position) => api.remote_seek(device, position.as_millis() as u64),
            RemoteCommand::Volume(volume) => {
                api.remote_volume(device, (volume.clamp(0.0, 1.0) * 100.0).round() as u8)
            }
            RemoteCommand::Queue(uri) => api.remote_queue(device, &uri),
        };
        if result.is_ok() {
            log::info!("remote: Spotify accepted the command");
        }
        if let Err(error) = result {
            log::warn!("Spotify refused a command: {error}");
            // The device may have closed; look for one again next time.
            self.device = None;
            self.alert("Spotify n'a pas pu exécuter la commande. L'appli Spotify officielle est-elle ouverte ?");
        }
        // Show the result without waiting a whole poll.
        thread::sleep(Duration::from_millis(300));
        self.poll();
    }

    fn poll(&mut self) {
        let state = match WebApi::global().remote_state() {
            Ok(state) => {
                self.poll_failed = false;
                state
            }
            Err(error) => {
                if !self.poll_failed {
                    log::warn!("could not read what Spotify plays: {error}");
                }
                self.poll_failed = true;
                return;
            }
        };
        let Some(state) = state else {
            self.playing = false;
            if self.shown.playing {
                self.shown.playing = false;
                self.submit(cmd::PLAYBACK_PAUSING, ());
            }
            return;
        };
        self.playing = state.is_playing;
        let item = state.item.as_ref().and_then(|item| {
            let kind = if item.kind == "episode" {
                ItemIdType::Podcast
            } else {
                ItemIdType::Track
            };
            ItemId::from_base62(item.id.as_deref()?, kind)
        });
        let progress = Duration::from_millis(state.progress_ms.unwrap_or(0));
        if item != self.shown.item {
            if let Some(item) = item {
                self.submit(cmd::PLAYBACK_PLAYING, (item, progress));
                if !state.is_playing {
                    self.submit(cmd::PLAYBACK_PAUSING, ());
                }
            }
            self.shown = Shown {
                item,
                playing: state.is_playing,
            };
            return;
        }
        self.submit(cmd::PLAYBACK_PROGRESS, progress);
        if state.is_playing != self.shown.playing {
            self.shown.playing = state.is_playing;
            if state.is_playing {
                self.submit(cmd::PLAYBACK_RESUMING, ());
            } else {
                self.submit(cmd::PLAYBACK_PAUSING, ());
            }
        }
    }
}
