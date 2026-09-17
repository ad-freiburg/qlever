add_library(metrics Metrics.cpp Resource.cpp ServerMetrics.cpp Tracing.cpp InstrumentedExecutor.h InstrumentedExecutor.cpp)
qlever_target_link_libraries(metrics qlever_util
        opentelemetry-cpp::api opentelemetry-cpp::sdk opentelemetry-cpp::metrics
        opentelemetry-cpp::trace opentelemetry-cpp::otlp_http_exporter
        opentelemetry-cpp::ostream_metrics_exporter opentelemetry-cpp::prometheus_exporter)
