add_library(metrics Metrics.cpp ServerMetrics.cpp)
qlever_target_link_libraries(metrics qlever_util
        opentelemetry-cpp::api opentelemetry-cpp::sdk opentelemetry-cpp::metrics
        opentelemetry-cpp::ostream_metrics_exporter opentelemetry-cpp::prometheus_exporter)
