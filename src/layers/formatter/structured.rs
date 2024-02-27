use crate::layers::formatter::errors::*;
use crate::layers::prelude::JsonStorage;
use serde::ser::SerializeMap;
use serde::Serializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use std::{fmt, io::Write};
use tracing::{span::Attributes, Event, Id, Level, Subscriber};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::registry::SpanRef;
use tracing_subscriber::Layer;
use time::format_description::well_known::Rfc3339;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Datatype {
    Constant(String),
    Level,
    Message,
    CurrentIso8601,
    CurrentMilliseconds,
    CurrentNanoseconds,
    TraceId,
    SpanId,
}

impl Datatype {
    fn from(data: &Value) -> Result<Datatype> {
        match *data {
            Value::Object(ref map) => match map.get("type") {
                Some(d) if d == "constant" => match map.get("value") {
                    Some(v) => Ok(Datatype::Constant(v.as_str().unwrap_or_default().into())),
                    _ => {
                        return Err(StructuredError::ParseError(
                            "Datatype missing 'value' at 'constant'".to_string(),
                        ))
                    }
                },
                Some(d) if d == "level" => Ok(Datatype::Level),
                Some(d) if d == "message" => Ok(Datatype::Message),
                Some(d) if d == "currentiso8601" => Ok(Datatype::CurrentIso8601),
                Some(d) if d == "currentmilliseconds" => Ok(Datatype::CurrentMilliseconds),
                Some(d) if d == "currentnanoseconds" => Ok(Datatype::CurrentNanoseconds),
                Some(d) if d == "traceid" => Ok(Datatype::TraceId),
                Some(d) if d == "spanid" => Ok(Datatype::SpanId),
                _ => {
                    return Err(StructuredError::ParseError(
                        "Unexpected json type for datatype value".to_string(),
                    ))
                }
            },
            _ => {
                return Err(StructuredError::ParseError(
                    "Unexpected type for datatype value".to_string(),
                ))
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Field {
    name: String,
    dtype: Datatype,
}

impl Field {
    ///
    /// Parse `field` definition
    pub fn from(data: &Value) -> Result<Self> {
        match *data {
            Value::Object(ref map) => {
                let name = match map.get("name") {
                    Some(&Value::String(ref name)) => name.to_string(),
                    _ => {
                        return Err(StructuredError::ParseError(
                            "Field missing 'name' attribute".to_string(),
                        ));
                    }
                };
                let dtype = match map.get("dtype") {
                    Some(v) => Datatype::from(v)?,
                    _ => {
                        return Err(StructuredError::ParseError(
                            "Field missing 'dtype' attribute".to_string(),
                        ));
                    }
                };

                Ok(Field { name, dtype })
            }
            _ => Err(StructuredError::ParseError(
                "Unexpected json type for field value".to_string(),
            )),
        }
    }
}

/// The type of record we are dealing with: entering a span, exiting a span, an event.
#[derive(Clone, Debug)]
pub enum SpanState {
    Enter,
    Exit,
    Event,
}

impl fmt::Display for SpanState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let repr = match self {
            SpanState::Enter => "start",
            SpanState::Exit => "end",
            SpanState::Event => "event",
        };
        write!(f, "{}", repr)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Structured<W>
where
    W: for<'writer> MakeWriter<'writer> + 'static,
{
    make_writer: W,
    pub(crate) fields: Vec<Field>,
}

impl<W> Structured<W>
where
    W: for<'writer> MakeWriter<'writer> + 'static,
{
    pub fn new<'d>(format: &'d str, writer: W) -> Result<Self> {
        let conf: Value = serde_json::from_str(format)
            .map_err(|_| StructuredError::ParseError("Config is not in json format".to_string()))?;

        match conf {
            Value::Object(ref structure) => {
                let fields = match structure.get("fields") {
                    Some(Value::Array(fields)) => fields
                        .iter()
                        .map(|f| Field::from(f))
                        .collect::<Result<_>>()?,
                    _ => {
                        return Err(StructuredError::ParseError(
                            "Fields should be an array".to_string(),
                        ));
                    }
                };

                Ok(Self {
                    fields,
                    make_writer: writer,
                })
            }
            _ => Err(StructuredError::ParseError(
                "Invalid format type".to_string(),
            )),
        }
    }

    fn structured_fields<S>(
        &self,
        ms: &mut impl SerializeMap<Error = serde_json::Error>,
        span: Option<&SpanRef<S>>,
        message: &str,
        level: &Level,
    ) -> Result<()>
    where
        S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        let now = time::OffsetDateTime::now_utc();

        self.fields.iter().try_for_each(|f| match &f.dtype {
            Datatype::Constant(s) => Ok(ms.serialize_entry(&f.name, &s)?),
            Datatype::Level => Ok(ms.serialize_entry(&f.name, &level.to_string())?),
            Datatype::Message => Ok(ms.serialize_entry(&f.name, message)?),
            Datatype::CurrentIso8601 => Ok(ms.serialize_entry(&f.name, &now.format(&Rfc3339)?)?),
            Datatype::CurrentMilliseconds => {
                Ok(ms.serialize_entry(&f.name, &(now.unix_timestamp_nanos() / 1_000_000))?)
            }
            Datatype::CurrentNanoseconds => {
                Ok(ms.serialize_entry(&f.name, &now.unix_timestamp_nanos())?)
            }
            Datatype::TraceId => {
                #[cfg(feature = "opentelemetry")]
                {
                    let trace_id = Self::extract_otel_trace_id(span);
                    if let Some(trace_id) = trace_id {
                        ms.serialize_entry(&f.name, &trace_id.to_string())?;
                    }
                }
                #[cfg(not(feature = "opentelemetry"))]
                {
                    let _ = span;
                }
                Ok(())
            }
            Datatype::SpanId => {
                #[cfg(feature = "opentelemetry")]
                {
                    let span_id = Self::extract_otel_span_id(span);
                    if let Some(span_id) = span_id {
                        ms.serialize_entry(&f.name, &span_id.to_string())?;
                    }
                }
                #[cfg(not(feature = "opentelemetry"))]
                {
                    let _ = span;
                }
                Ok(())
            }
        })
    }

    #[cfg(feature = "opentelemetry")]
    fn extract_otel_span_id<S>(span: Option<&SpanRef<S>>) -> Option<opentelemetry::trace::SpanId>
    where
        S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        let span = span?;
        let extensions = span.extensions();
        let otel = extensions.get::<tracing_opentelemetry::OtelData>()?;
        let span_id = otel.builder.span_id?;
        Some(span_id)
    }

    #[cfg(feature = "opentelemetry")]
    fn extract_otel_trace_id<S>(span: Option<&SpanRef<S>>) -> Option<opentelemetry::trace::TraceId>
    where
        S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        for span in span?.scope() {
            let extensions = span.extensions();
            if let Some(otel) = extensions.get::<tracing_opentelemetry::OtelData>() {
                if let Some(trace_id) = otel.builder.trace_id {
                    return Some(trace_id);
                }
            };
        }

        None
    }

    fn format_span_context<S>(&self, span: &SpanRef<S>, state: SpanState) -> String
    where
        S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        format!("[{}::{} - {}]", span.metadata().module_path().unwrap_or("(unknown)"), span.metadata().name(), state)
    }

    fn format_event_message<S>(
        &self,
        current_span: &Option<SpanRef<S>>,
        event: &Event,
        event_visitor: &JsonStorage<'_>,
    ) -> String
    where
        S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        // Extract the "message" field, if provided. Fallback to the target, if missing.
        let mut message = event_visitor
            .values()
            .get("message")
            .map(|v| match v {
                Value::String(s) => Some(s.as_str()),
                _ => None,
            })
            .flatten()
            .unwrap_or_else(|| event.metadata().target())
            .to_owned();

        // If the event is in the context of a span, prepend the span name to the message.
        if let Some(span) = &current_span {
            message = format!(
                "{} {}",
                self.format_span_context(span, SpanState::Event),
                message
            );
        }

        message
    }

    fn format<S>(
        &self,
        event: &Event<'_>,
        current_span: Option<SpanRef<S>>,
        event_visitor: JsonStorage,
    ) -> Result<Vec<u8>>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let mut buffer = Vec::with_capacity(self.fields.len() * 128);

        let mut serializer = serde_json::Serializer::new(&mut buffer);
        let mut map_serializer = serializer.serialize_map(None)?;

        let message = self.format_event_message(&current_span, event, &event_visitor);
        self.structured_fields(
            &mut map_serializer,
            current_span.as_ref(),
            &message,
            event.metadata().level(),
        )?;

        // Add all the other fields associated with the event, expect the message we already used.
        let _ = event_visitor
            .values()
            .iter()
            .filter(|(&key, _)| key != "message")
            .try_for_each(|(key, value)| -> Result<()> {
                Ok(map_serializer.serialize_entry(key, value)?)
            });

        // Add all the fields from the current span, if we have one.
        if let Some(span) = &current_span {
            let extensions = span.extensions();
            if let Some(visitor) = extensions.get::<JsonStorage>() {
                let _ = visitor
                    .values()
                    .iter()
                    .try_for_each(|(key, value)| -> Result<()> {
                        Ok(map_serializer.serialize_entry(key, value)?)
                    });
            }
        }
        map_serializer.end()?;
        Ok(buffer)
    }

    fn serialize_span<S>(&self, span: &SpanRef<S>, state: SpanState) -> Result<Vec<u8>>
    where
        S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        let mut buffer = Vec::with_capacity(self.fields.len() * 128);
        let mut serializer = serde_json::Serializer::new(&mut buffer);
        let mut map_serializer = serializer.serialize_map(None)?;
        let message = self.format_span_context(&span, state);
        self.structured_fields(
            &mut map_serializer,
            Some(span),
            &message,
            span.metadata().level(),
        )?;

        let extensions = span.extensions();
        if let Some(visitor) = extensions.get::<JsonStorage>() {
            for (key, value) in visitor.values() {
                map_serializer.serialize_entry(key, value)?;
            }
        }
        map_serializer.end()?;
        Ok(buffer)
    }

    fn emit(&self, mut buffer: Vec<u8>) -> Result<()> {
        buffer.write_all(b"\n")?;
        self.make_writer
            .make_writer()
            .write_all(&buffer)
            .map_err(|e| StructuredError::WriterError(e.to_string()))
    }
}

impl<S, W> Layer<S> for Structured<W>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: for<'writer> MakeWriter<'writer> + 'static,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let current_span = ctx.lookup_current();

        let mut event_visitor = JsonStorage::default();
        event.record(&mut event_visitor);

        let _ = self
            .format(event, current_span, event_visitor)
            .map(|formatted| {
                let _ = self.emit(formatted);
            });
    }

    fn on_new_span(&self, _attrs: &Attributes, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("Span not found, this is a bug");
        if let Ok(serialized) = self.serialize_span(&span, SpanState::Enter) {
            let _ = self.emit(serialized);
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("Span not found, this is a bug");
        if let Ok(serialized) = self.serialize_span(&span, SpanState::Exit) {
            let _ = self.emit(serialized);
        }
    }
}

#[cfg(test)]
mod tracing_json_structured_tests {
    use super::*;

    #[test]
    fn parse_structured() {
        let config: &str = r#"
        {
            "fields": [
                {
                    "name": "v",
                    "dtype": {
                      "type": "constant",
                      "value": "1"
                    }
                },
                {
                    "name": "l",
                    "dtype": {
                      "type": "level",
                      "value": "WARN"
                    }
                },
                {
                    "name": "current_ms",
                    "dtype": {
                      "type": "currentmilliseconds"
                    }
                }
            ]
        }
        "#;

        let _s = Structured::new(config, std::io::stdout).unwrap();
    }
}
