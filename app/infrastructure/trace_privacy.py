"""Export trace structure and numeric usage, never prompts, tool arguments or PII."""
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter
from opentelemetry.trace import Status


class MetadataOnlyExporter(SpanExporter):
    def __init__(self, delegate: SpanExporter):
        self.delegate = delegate

    def export(self, spans):
        safe = []
        for span in spans:
            attributes = {
                key: value for key, value in (span.attributes or {}).items()
                if key in {"gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens",
                    "agentscope.usage.cache_input_tokens", "agentscope.usage.cache_creation_input_tokens"}
                and isinstance(value, (int, float)) and not isinstance(value, bool)
            }
            safe.append(ReadableSpan(name="globex.operation", context=span.context,
                parent=span.parent, resource=Resource({"service.name": "globex-agent"}),
                attributes=attributes, events=(), links=(), kind=span.kind,
                status=Status(span.status.status_code), start_time=span.start_time,
                end_time=span.end_time))
        return self.delegate.export(safe)

    def shutdown(self):
        return self.delegate.shutdown()

    def force_flush(self, timeout_millis=30000):
        return self.delegate.force_flush(timeout_millis)
