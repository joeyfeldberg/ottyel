use anyhow::Result;

use crate::{
    domain::{DashboardSnapshot, OverviewStats},
    store::Store,
};

#[derive(Debug, Clone)]
pub struct QueryService {
    store: Store,
    page_size: usize,
}

#[derive(Debug, Clone)]
pub struct QueryFilters {
    pub service: Option<String>,
    pub errors_only: bool,
    pub time_window: TimeWindow,
    pub search_query: Option<String>,
    pub log_filters: LogFilters,
}

#[derive(Debug, Clone, Default)]
pub struct LogFilters {
    pub severity: LogSeverityFilter,
    pub correlation: LogCorrelationFilter,
    pub search_query: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum LogSeverityFilter {
    #[default]
    All,
    Error,
    Warn,
    Info,
    Debug,
}

impl LogSeverityFilter {
    pub const ALL: [Self; 5] = [Self::All, Self::Error, Self::Warn, Self::Info, Self::Debug];

    pub fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug+trace",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum LogCorrelationFilter {
    #[default]
    All,
    TraceLinked,
    SpanLinked,
    Uncorrelated,
}

impl LogCorrelationFilter {
    pub const ALL: [Self; 4] = [
        Self::All,
        Self::TraceLinked,
        Self::SpanLinked,
        Self::Uncorrelated,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::TraceLinked => "trace-linked",
            Self::SpanLinked => "span-linked",
            Self::Uncorrelated => "uncorrelated",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TimeWindow {
    FifteenMinutes,
    OneHour,
    SixHours,
    TwentyFourHours,
    All,
}

impl TimeWindow {
    pub const ALL: [Self; 5] = [
        Self::FifteenMinutes,
        Self::OneHour,
        Self::SixHours,
        Self::TwentyFourHours,
        Self::All,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::FifteenMinutes => "15m",
            Self::OneHour => "1h",
            Self::SixHours => "6h",
            Self::TwentyFourHours => "24h",
            Self::All => "all",
        }
    }

    pub fn threshold_unix_nano(self) -> Option<i64> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        let nanos = match self {
            Self::FifteenMinutes => Some(15 * 60 * 1_000_000_000_i64),
            Self::OneHour => Some(60 * 60 * 1_000_000_000_i64),
            Self::SixHours => Some(6 * 60 * 60 * 1_000_000_000_i64),
            Self::TwentyFourHours => Some(24 * 60 * 60 * 1_000_000_000_i64),
            Self::All => None,
        }?;
        Some(now.saturating_sub(nanos))
    }
}

impl QueryService {
    pub fn new(store: Store, page_size: usize) -> Self {
        Self { store, page_size }
    }

    pub fn snapshot(&self, filters: &QueryFilters) -> Result<DashboardSnapshot> {
        let threshold = filters.time_window.threshold_unix_nano();
        let services = self.store.services(threshold)?;
        let (trace_count, error_span_count, log_count, metric_count, llm_count) =
            self.store.counts(threshold)?;
        let traces = self.store.recent_traces(
            filters.service.as_deref(),
            filters.errors_only,
            self.page_size,
            threshold,
            filters.search_query.as_deref(),
        )?;
        let selected_trace = traces
            .first()
            .map(|summary| self.store.trace_detail(&summary.trace_id))
            .transpose()?
            .unwrap_or_default();
        let logs = self.store.recent_logs(
            filters.service.as_deref(),
            self.page_size,
            threshold,
            filters.search_query.as_deref(),
            &filters.log_filters,
        )?;
        let metrics = self.store.recent_metrics(
            filters.service.as_deref(),
            self.page_size,
            threshold,
            filters.search_query.as_deref(),
        )?;
        let llm = self.store.recent_llm(
            filters.service.as_deref(),
            self.page_size,
            threshold,
            filters.search_query.as_deref(),
        )?;

        Ok(DashboardSnapshot {
            services: services.clone(),
            overview: OverviewStats {
                service_count: services.len(),
                trace_count,
                error_span_count,
                log_count,
                metric_count,
                llm_count,
            },
            traces,
            selected_trace,
            logs,
            metrics,
            llm,
        })
    }

    pub fn trace_detail(&self, trace_id: &str) -> Result<Vec<crate::domain::SpanDetail>> {
        self.store.trace_detail(trace_id)
    }
}
