from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


def test_exporter_keeps_usage_and_context_but_never_exports_content():
    from app.infrastructure.trace_privacy import MetadataOnlyExporter
    sink = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(MetadataOnlyExporter(sink)))
    with provider.get_tracer("fixture").start_as_current_span("secret@example.test") as span:
        span.set_attribute("gen_ai.usage.input_tokens", 12)
        span.set_attribute("gen_ai.input.messages", "Bearer private-key")
        span.set_attribute("gen_ai.tool.call.arguments", '{"phone":"1234"}')
        span.add_event("error", {"exception.message": "private-key"})
    exported = sink.get_finished_spans()[0]
    assert exported.context.trace_id > 0
    assert exported.attributes == {"gen_ai.usage.input_tokens": 12}
    assert exported.name == "globex.operation"
    assert exported.events == ()
    assert "private-key" not in str(exported.attributes)
