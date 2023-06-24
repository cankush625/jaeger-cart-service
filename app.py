import requests
from flask import Flask
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from werkzeug.exceptions import HTTPException

app = Flask(__name__)

resource = Resource.create({"service.name": "cart_service"})
jaeger_exporter = JaegerExporter(agent_host_name="192.168.59.100", agent_port=31635)
provider = TracerProvider(
    resource=resource,
    active_span_processor=BatchSpanProcessor(jaeger_exporter),
)
trace.set_tracer_provider(provider)
FlaskInstrumentor().instrument_app(app)

PRODUCT_BASE_URL = "http://localhost:8000/api/v1/products"
PAYMENT_BASE_URL = "http://localhost:8001/api/v1/payment"


class ProductNotFoundException(HTTPException):
    code = 404
    description = "Product not found"


@app.route("/cart/<product_id>", methods=["POST"])
def add_item_to_cart(product_id):
    with trace.get_tracer(__name__).start_as_current_span("add-item-to-cart"):
        # Jaeger does not link the spans to parent span automatically.
        # We need to provide the carrier information in the request header
        # and after that Jaeger will associate correct child span to
        # its parent span.
        # Ref: https://opentelemetry.io/docs/instrumentation/python/cookbook/
        carrier = {}
        # Write the current context into the carrier.
        TraceContextTextMapPropagator().inject(carrier)
        url = PRODUCT_BASE_URL + f"/{product_id}"
        response = requests.get(url=url, headers=carrier)
        if response.ok:
            return f"Product with id {product_id} is added to cart"
        raise ProductNotFoundException()


@app.route("/cart", methods=["GET"])
def get_cart():
    with trace.get_tracer(__name__).start_as_current_span("get-cart"):
        user_id = 1  # get current user
        url = PAYMENT_BASE_URL + f"/cart/{user_id}"
        response = requests.get(url=url)
        if response.ok:
            return {"success": "Cart items retrieved successfully."}
        if response.status_code == 404:
            raise ConnectionError("Cannot connect to payment service.")
        return {"failed": "Failed to retrieve cart items."}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
